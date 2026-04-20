/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import lombok.NonNull;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.api.RunnableBlobStoreTask;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TaskSuspensionSupport.TaskFailedDueToTooManyRetriesException;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TaskSuspensionSupport.TaskSuspended;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;

public abstract class BaseTask implements RunnableBlobStoreTask
{
    protected BaseTask( 
            @NonNull final BlobStoreTaskPriority priority,
            @NonNull final BeansServiceManager beansServiceManager )
    {
        this( priority, beansServiceManager, 3 );
    }

    // This constructor is used for testing when a larger max retries is needed in order for tests to run in a
    // timely fashion.
    protected BaseTask(
            @NonNull final BlobStoreTaskPriority priority,
            @NonNull final BeansServiceManager serviceManager,
            final int maxRetriesBeforeSuspensionRequired )
    {
        m_priority = priority;
        m_serviceManager = serviceManager;
        m_suspensionSupport = new TaskSuspensionSupport( maxRetriesBeforeSuspensionRequired, null, 1000 * 60 * 5, 1000 * 60 * 2 );
        Validations.verifyNotNull( "Priority", m_priority );
        Validations.verifyNotNull( "ServiceManager", m_serviceManager );
    }
    
    
    final protected void preparedForExecution()
    {
        if ( BlobStoreTaskState.READY != m_state )
        {
            throw new IllegalStateException( 
                    "Cannot transist to " + BlobStoreTaskState.PENDING_EXECUTION 
                    + " while in state " + m_state + "." );
        }
        
        handlePreparedForExecution();
        m_state = BlobStoreTaskState.PENDING_EXECUTION;
    }
    
    
    protected void handlePreparedForExecution()
    {
        // empty
    }
    
    
    final public void run()
    {
        Thread.currentThread().setName( getClass().getSimpleName() + "#" + getId() );
        try
        {
            if ( BlobStoreTaskState.PENDING_EXECUTION != m_state )
            {
                throw new IllegalStateException( "Cannot start when task is in state " + m_state + "." );
            }
            
            m_state = BlobStoreTaskState.IN_PROGRESS;
            m_durationInProgress = new Duration();
            m_dateStarted = new Date();
            performPreRunValidations();
            final BlobStoreTaskState completionState = runInternal();
            
            if ( BlobStoreTaskState.COMPLETED != completionState 
                    && BlobStoreTaskState.READY != completionState )
            {
                throw new RuntimeException( "Completion task state cannot be " + completionState + "." );
            }
            transistToCompleted( completionState );
        }
        catch ( final RuntimeException ex )
        {
            final boolean transistStateToReady =
                    ( BlobStoreTaskState.COMPLETED != m_state && BlobStoreTaskState.READY != m_state );
            
            LOG.warn( "Task failed due to exception.  " + ( ( transistStateToReady ) ? 
                    "Will transist task's state from " + m_state + " to " + BlobStoreTaskState.READY + "."
                    : "Will keep task's state of " + m_state + "." ) );
            handleExecutionFailed();
            if ( transistStateToReady )
            {
                transistToCompleted( BlobStoreTaskState.READY );
            }
            throw ex;
        }
        finally
        {
            m_dateStarted = null;
            m_durationInProgress = null;
        }
    }
    
    
    abstract protected void performPreRunValidations();
    
    
    /**
     * @return the new task state
     */
    abstract protected BlobStoreTaskState runInternal();
    
    
    final protected void doNotTreatReadyReturnValueAsFailure()
    {
        m_doNotTreatReadyReturnValueAsFailure = true;
    }
    
    
    final protected void invalidateTaskAndThrow(final RuntimeException cause )
    {
        invalidateTaskInternal( new BlobStoreTaskNoLongerValidException( cause ) );
    }
    
    
    final protected void invalidateTaskAndThrow(final String cause )
    {
        invalidateTaskInternal( new BlobStoreTaskNoLongerValidException( cause ) );
    }
    
    
    private void invalidateTaskInternal( final BlobStoreTaskNoLongerValidException ex )
    {
        if ( BlobStoreTaskState.READY != m_state )
        {
            final IllegalStateException e = new IllegalStateException(
                    "This method can only be called if the task is in state "
                            + BlobStoreTaskState.READY + "." );
            e.addSuppressed( ex );
            throw e;
        }
        
        m_state = BlobStoreTaskState.COMPLETED;
        throw ex;
    }
    
    
    final public void executionFailed(RuntimeException e)
    {
        if ( BlobStoreTaskState.PENDING_EXECUTION != m_state )
        {
            final IllegalStateException ex = new IllegalStateException( "Cannot fail to run when task is in state " + m_state + "." );
            ex.addSuppressed(e);
            throw ex;
        }
        
        m_state = BlobStoreTaskState.IN_PROGRESS;
        transistToCompleted( BlobStoreTaskState.READY );
        handleExecutionFailed();
    }
    
    
    protected void handleExecutionFailed()
    {
        // empty
    }
    
    
    final protected void requireTaskSuspension()
    {
        while ( TaskSuspended.YES != m_suspensionSupport.getState() )
        {
            registerFailureWithSuspensionSupport();
        }
    }
    
    
    private void transistToCompleted( final BlobStoreTaskState completionState )
    {
        if ( BlobStoreTaskState.IN_PROGRESS != m_state )
        {
            throw new IllegalStateException( 
                    "Cannot transist to " + completionState + " when task is in state " + m_state + "." );
        }
        
        m_state = completionState;
        
        LOG.info( "Task has finished executing.  New state is " + completionState + "." );
        if ( BlobStoreTaskState.READY == m_state && !m_doNotTreatReadyReturnValueAsFailure )
        {
            m_doNotTreatReadyReturnValueAsFailure = false;
            registerFailureWithSuspensionSupport();
        }
        
        taskSchedulingRequired();
    }
    
    
    private void registerFailureWithSuspensionSupport()
    {
        try
        {
            m_suspensionSupport.failureOccurred();
        }
        catch ( final TaskFailedDueToTooManyRetriesException ex )
        {
            LOG.error( toString() + " cannot be retried any longer.", ex );
            m_state = BlobStoreTaskState.COMPLETED;
            taskCancelledDueToTooManyRunFailures();
        }
    }
    
    
    protected void taskCancelledDueToTooManyRunFailures()
    {
        throw new UnsupportedOperationException( "Looks like I need implementing." );
    }
    
    
    public void dequeued()
    {
    	//default implementation does nothing
    }
    
    
    private void taskSchedulingRequired()
    {
        final String prefix = 
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( m_state.toString() );
        Thread.currentThread().setName( prefix + "-" + Thread.currentThread().getName() );
        for ( final BlobStoreTaskSchedulingListener listener : m_schedulingListeners )
        {
            listener.taskSchedulingRequired( this );
        }
    }
    
    
    final public void addSchedulingListener( final BlobStoreTaskSchedulingListener listener )
    {
        if ( m_schedulingListeners.contains( listener ) )
        {
            return;
        }
        
        m_schedulingListeners.add( listener );
    }
    
    
    public final long getId()
    {
        return m_id;
    }
    
    
    public Date getDateScheduled()
    {
        return new Date( m_dateCreated.getTime() );
    }
    
    
    public Date getDateStarted()
    {
        return ( null == m_dateStarted ) ? null : new Date( m_dateStarted.getTime() );
    }
    
    
    public Duration getDurationScheduled()
    {
        return m_durationScheduled;
    }
    
    
    public Duration getDurationInProgress()
    {
        return m_durationInProgress;
    }
    
    
    final public String getName()
    {
        return getClass().getSimpleName();
    }
    
    
    final public BlobStoreTaskPriority getPriority()
    {
        return m_priority;
    }
    
    
    final public BaseTask setPriority( final BlobStoreTaskPriority value )
    {
        m_priority = value;
        return this;
    }
    

    final public BlobStoreTaskState getState()
    {
        if ( BlobStoreTaskState.READY == m_state && TaskSuspended.YES == m_suspensionSupport.getState() )
        {
            return BlobStoreTaskState.NOT_READY;
        }
        return m_state;
    }
    
    
    final protected BlobStoreTaskState getRawState()
    {
        return m_state;
    }
    
    
    final public BeansServiceManager getServiceManager()
    {
        if ( null == m_serviceManager )
        {
            throw new IllegalStateException( "Service manager hasn't been set yet." );
        }
        return m_serviceManager;
    }
    
    
    @Override
    final public String toString()
    {
        return getName() + "#" + m_id + "[" + getDescription() + "]";
    }
    

    protected final BeansServiceManager m_serviceManager;
    private volatile BlobStoreTaskState m_state = BlobStoreTaskState.READY;
    private volatile Duration m_durationInProgress;
    private volatile Date m_dateStarted;
    private volatile BlobStoreTaskPriority m_priority;
    private volatile boolean m_doNotTreatReadyReturnValueAsFailure;
    
    private final long m_id = NEXT_ID.getAndIncrement();
    private final Date m_dateCreated = new Date();
    private final Duration m_durationScheduled = new Duration();
    private final TaskSuspensionSupport m_suspensionSupport;
    private final List< BlobStoreTaskSchedulingListener > m_schedulingListeners = 
            new CopyOnWriteArrayList<>();
    
    private final static AtomicLong NEXT_ID = new AtomicLong( 1 );
    protected final static Logger LOG = Logger.getLogger( BaseTask.class );
}
