/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import lombok.NonNull;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.SystemFailureService;
import com.spectralogic.s3.common.dao.service.target.TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetTask;
import com.spectralogic.s3.dataplanner.backend.target.task.BaseWriteChunkToPublicCloudTask;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnable;
import com.spectralogic.util.thread.ThrottledRunnableExecutor;
import com.spectralogic.util.thread.ThrottledRunnableExecutor.WhenAggregating;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

abstract class BaseTargetBlobStore
    < T extends DatabasePersistable & ReplicationTarget< T >, CF, FS extends TargetFailureService< ? > >
    extends BaseShutdownable implements TargetBlobStore
{
    protected BaseTargetBlobStore(
            final Class< T > targetType,
            final Class< FS > failureServiceType,
            final CF connectionFactory,
            final FeatureKeyType featureKeyRequirementForWrites,
            final SystemFailureType featureKeyRestrictionFailure,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Target type", targetType );
        Validations.verifyNotNull( "Failure service type", failureServiceType );
        Validations.verifyNotNull( "Connection factory", connectionFactory );
        Validations.verifyNotNull( "Cache manager", diskManager );
        Validations.verifyNotNull( "Job progress manager", jobProgressManager );
        Validations.verifyNotNull( "Service manager", serviceManager );
        
        m_targetType = targetType;
        m_failureService = serviceManager.getService( failureServiceType );
        m_connectionFactory = connectionFactory;
        m_featureKeyRequirementForWrites = featureKeyRequirementForWrites;
        m_featureKeyRestrictionFailure = featureKeyRestrictionFailure;
        m_diskManager = diskManager;
        m_jobProgressManager = jobProgressManager;
        m_serviceManager = serviceManager;
        m_taskWorkPool = WorkPoolFactory.createWorkPool( 12, m_targetType.getSimpleName() + "TaskExecutor" );
        m_offlineDataStagingWindowManager =
                new OfflineDataStagingWindowManagerImpl( m_serviceManager, 1000 * 60 );
        
        addShutdownListener( m_periodicTargetTaskStarterExecutor );
        
        m_periodicTargetTaskStarterExecutor.start();
    }


    synchronized final protected void addIoTask( final TargetTask< T, CF > ioTask )
    {
        m_ioTasks.add( ioTask );
        enqueued( ioTask );
    }
    
    
    synchronized final protected void addImportTask(
            final UUID targetId, 
            final TargetTask< T, CF > importTask )
    {
        if ( m_importTasks.containsKey( targetId ) )
        {
            deleteTask( m_importTasks.get( targetId ), "scheduling new import task" );
        }
        
        m_importTasks.put( targetId, importTask );
        enqueued( importTask );
    }
    
    
    final public TargetTask< T, CF > enqueued( final TargetTask< T, CF > task )
    {
        task.addSchedulingListener( new TargetTaskSchedulingRequiredListener() );
        LOG.info( "Enqueued " + task.toString() + " for execution at priority " + task.getPriority() + "." );
        return task;
    }
    
    
    private final class TargetTaskSchedulingRequiredListener implements BlobStoreTaskSchedulingListener
    {
        public void taskSchedulingRequired( final BlobStoreTask task )
        {
            m_periodicTargetTaskStarter.run();
        }
    } // end inner class def

    
    final public void verify(@NonNull final BlobStoreTaskPriority priority, final UUID persistenceTargetId )
    {
        throw new UnsupportedOperationException(
                "There is no concept of persistence target verification." );
    }

    
    synchronized private List< TargetTask< T, CF > > getSortedTasks()
    {
        final List< TargetTask< T, CF > > retval = new ArrayList<>();
        retval.addAll( m_ioTasks );
        retval.addAll( m_importTasks.values() );
        retval.sort(new TargetTaskComparator());
        return retval;
    }

    
    final public Set< BlobStoreTask > getTasks()
    {
        return new HashSet<>(getSortedTasks());
    }

    @Override
    final public Set<? extends BlobStoreTask> getTasksForJob(final UUID jobId)
    {
        final Set< BlobStoreTask > retval = new HashSet<>();
        for ( final BlobStoreTask task : getSortedTasks() )
        {
            final UUID[] jobIds = task.getJobIds();
            if ( null != jobIds )
            {
                for ( final UUID id : jobIds )
                {
                    if ( jobId.equals( id ) )
                    {
                        retval.add( task );
                        break;
                    }
                }
            }
        }
        return retval;
    }


    synchronized private void deleteTask( final TargetTask< T, CF > task, final String cause )
    {
        if ( m_ioTasks.remove( task ) )
        {
            dequeued( task, cause );
        }
        else if ( m_importTasks.containsValue( task ) )
        {
            for ( final Map.Entry< UUID, TargetTask< T, CF > > e : new HashSet<>( m_importTasks.entrySet() ) )
            {
                if ( e.getValue() == task )
                {
                    m_importTasks.remove( e.getKey() );
                    dequeued( task, cause );
                    return;
                }
            }
        }
    }
    
    
    private TargetTask< T, CF > dequeued( final TargetTask< T, CF > task, final String cause )
    {
        if ( BlobStoreTaskState.PENDING_EXECUTION == task.getState() 
                || BlobStoreTaskState.IN_PROGRESS == task.getState() )
        {
            throw new IllegalStateException(
                    "Cannot dequeue " + task + " while it is in state " + task.getState() + "." );
        }
        
        LOG.info( "Dequeued " + task.toString() + " from execution since " + cause + "." );
        task.dequeued();
        return task;
    }
    
    
    private final static class TargetTaskComparator implements Comparator< TargetTask< ?, ? > >
    {
        public int compare( final TargetTask< ?, ? > t1, final TargetTask< ?, ? > t2 )
        {
            if ( t1.getPriority() != t2.getPriority() )
            {
                return t2.getPriority().ordinal() - t1.getPriority().ordinal();
            }
            return (int)( t2.getId() - t1.getId() );
        }
    } // end inner class def

    
    final public void refreshEnvironmentNow()
    {
        updateTargetState();
    }
    
    
    private final class PeriodicTargetTaskStarter implements Runnable
    {
        public void run()
        {
            m_targetTaskStarterExecutor.add( m_targetTaskStarter );
        }
    } // end inner class def
    
    
    private final class TargetTaskStarter implements ThrottledRunnable
    {
        /**
         * In any env (test and non-test) only a single instance of this class
         * can ever be created. However, in test envs it's possible to invoke
         * this method reflectively, thus the singleton property can be violated
         * in test envs--this is the only reason why we synchronize this method.
         * Do not remove the synchronzie.
         */
        public synchronized void run( final RunnableCompletionNotifier completionNotifier )
        {
            final String threadName = m_targetType.getSimpleName() + "TaskStarter";
            final Duration duration = new Duration();
            try
            {
                Thread.currentThread().setName( threadName );
                if ( !m_serviceManager.getService( DataPathBackendService.class ).isActivated() )
                {
                    LOG.info( "Will not attempt to start any tasks since backend isn't activated." );
                    return;
                }

                updateTargetState();
                discoverWork();
                final List< TargetTask< T, CF > > tasks = getSortedTasks();
                if ( tasks.isEmpty() )
                {
                    LOG.info( "There are no outstanding target tasks." );
                }
                else
                {
                    LOG.info( "Will attempt to start tasks.  There are " + tasks.size() + " tasks." );
                }

                final boolean cloudOutAllowed;
                if ( null == m_featureKeyRequirementForWrites )
                {
                    cloudOutAllowed = true;
                }
                else
                {
                    cloudOutAllowed = 
                            ( 0 < m_serviceManager.getRetriever( FeatureKey.class ).getCount( Require.all( 
                                    Require.beanPropertyEquals( 
                                            ErrorMessageObservable.ERROR_MESSAGE, null ),
                                    Require.beanPropertyEquals( 
                                            FeatureKey.KEY, m_featureKeyRequirementForWrites ) ) ) );
                    if ( cloudOutAllowed )
                    {
                        LOG.info( "Cloud out has a valid license." );
                        m_serviceManager.getService( SystemFailureService.class ).deleteAll( 
                                m_featureKeyRestrictionFailure );
                    }
                    else if ( tasks.isEmpty() )
                    {
                        LOG.info( "Cloud is not in use." );
                        m_serviceManager.getService( SystemFailureService.class ).deleteAll( 
                                m_featureKeyRestrictionFailure );
                    }
                    else
                    {
                        LOG.warn( "Cloud out is disabled due to lack of a valid license." );
                        m_serviceManager.getService( SystemFailureService.class ).create( 
                                m_featureKeyRestrictionFailure,
                                "Replication to the cloud is not enabled.", 
                                Integer.valueOf( 60 * 24 ) );
                    }
                }
                for ( final TargetTask< T, CF > task : tasks )
                {
                    if ( BlobStoreTaskState.COMPLETED == task.getState() )
                    {
                        deleteTask( task, "task has completed" );
                    }
                    else if ( BlobStoreTaskState.READY == task.getState() )
                    {
                        if ( m_taskWorkPool.isFull() )
                        {
                            LOG.info( "Task work pool is full.  " 
                                      + "Will not attempt to execute any more tasks at this time." );
                            return;
                        }
                        if ( cloudOutAllowed || !BaseWriteChunkToPublicCloudTask.class.isAssignableFrom( 
                                task.getClass() ) )
                        {
                            attemptToExecute( task );
                        }
                    }
                }
            }
            finally
            {
                LOG.info( "Completed running " + threadName + " in " + duration + "." );
                completionNotifier.completed();
            }
        }
    } // end inner class def

    protected abstract void discoverWork();


    synchronized private void updateTargetState()
    {
        for ( final T target : m_serviceManager.getRetriever( m_targetType ).retrieveAll().toSet() )
        {
            try
            {
                if ( Quiesced.NO == target.getQuiesced() )
                {
                    verifyCanConnect( target );
                    m_failureService.deleteAll( target.getId(), TargetFailureType.NOT_ONLINE );
                }
            }
            catch ( final RuntimeException ex )
            {
                LOG.info( m_targetType.getSimpleName() + " " + target.getId() + " is not online.", ex );
                m_failureService.create( 
                        target.getId(), TargetFailureType.NOT_ONLINE, ex, Integer.valueOf( 60 ) );
            }
            
            if ( Quiesced.PENDING == target.getQuiesced() && !isActiveTaskForTarget( target.getId() ) )
            {
                m_serviceManager.getUpdater( m_targetType ).update(
                        target.setQuiesced( Quiesced.YES ),
                        ReplicationTarget.QUIESCED );
            }
        }
    }
    
    
    private boolean isActiveTaskForTarget( final UUID targetId )
    {
        for ( final BlobStoreTask task : getTasks() )
        {
            if ( BlobStoreTaskState.NOT_READY == task.getState()
                    || BlobStoreTaskState.READY == task.getState()
                    || BlobStoreTaskState.COMPLETED == task.getState()
                    || !targetId.equals( task.getTargetId() ) )
            {
                continue;
            }
            return true;
        }
        
        return false;
    }
    
    
    protected abstract void verifyCanConnect( final T target );
    
    
    private void attemptToExecute( final TargetTask< T, CF > task )
    {
        task.prepareForExecutionIfPossible();
        if ( BlobStoreTaskState.PENDING_EXECUTION != task.getState() )
        {
            return;
        }
        
        final ReplicationTarget< ? > target = task.getTarget();
        LOG.info( Platform.NEWLINE + LogUtil.getLogMessageHeaderBlock( "Execute " + task )
                + Platform.NEWLINE + "      Target: " + task.getTargetId() 
                + " (" + target.getName() + ")"             
                + Platform.NEWLINE + "    Priority: " + task.getPriority() + Platform.NEWLINE );
        m_taskWorkPool.submit( task );
    }

    @Override
    public void taskSchedulingRequired() {
        m_targetTaskStarterExecutor.add( m_targetTaskStarter );
    }

    
    //private final BasicIoTasks< TargetTask< T, CF > > m_ioTasks = new BasicIoTasks<>();
    LinkedHashSet< TargetTask< T, CF > > m_ioTasks = new LinkedHashSet<>();
    private final Map< UUID, TargetTask< T, CF > > m_importTasks = new HashMap<>();

    protected final CF m_connectionFactory;
    private final FeatureKeyType m_featureKeyRequirementForWrites;
    private final SystemFailureType m_featureKeyRestrictionFailure;
    protected final DiskManager m_diskManager;
    protected final JobProgressManager m_jobProgressManager;
    protected final BeansServiceManager m_serviceManager;
    protected final OfflineDataStagingWindowManagerImpl m_offlineDataStagingWindowManager;
    private final FS m_failureService;
    private final Class< T > m_targetType;

    private final WorkPool m_taskWorkPool;
    /** for testing */ final static String FIELD_TARGET_TASK_STARTER = "m_targetTaskStarter";
    private final TargetTaskStarter m_targetTaskStarter = new TargetTaskStarter();
    private final ThrottledRunnableExecutor< TargetTaskStarter > m_targetTaskStarterExecutor = 
            new ThrottledRunnableExecutor<>( 20, null, WhenAggregating.DELAY_EXECUTION );
    
    private final PeriodicTargetTaskStarter m_periodicTargetTaskStarter = new PeriodicTargetTaskStarter();
    private final RecurringRunnableExecutor m_periodicTargetTaskStarterExecutor = 
            new RecurringRunnableExecutor(
                    m_periodicTargetTaskStarter, 
                    140000 + PERIODIC_RUN_OFFSET.getAndIncrement() * 5000 );
    
    private final static AtomicInteger PERIODIC_RUN_OFFSET = new AtomicInteger( 0 );
    private final static Logger LOG = Logger.getLogger( BaseTargetBlobStore.class );
}
