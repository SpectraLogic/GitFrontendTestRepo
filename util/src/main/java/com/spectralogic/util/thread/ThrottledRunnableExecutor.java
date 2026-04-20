/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.util.Timer;
import java.util.TimerTask;

import com.spectralogic.util.lang.Validations;
import org.apache.log4j.Logger;

import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.io.lang.HeapDumper;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.thread.ThrottledRunnable.RunnableCompletionNotifier;
import com.spectralogic.util.thread.ThrottledRunnable.ThrottledRunnableAggregator;

/**
 * Executes a runnable with a delay such that additional Runnables that come in while the
 * initial Runnable has not executed will be aggregated with the already-scheduled one, so
 * that only one Runnable gets executed once the delay has elapsed.
 */
public final class ThrottledRunnableExecutor< T extends ThrottledRunnable >
{
    /**
     * @param delayInMillis - Required, a Runnable will be delayed from executing for this many
     * millis at a time until there were no new Runnables that came in since the last delay
     * 
     * @param delayedRunnableAggregator - Optional, if not provided, Runnables that come in
     * while there is a scheduled-to-run Runnable will be dropped on the floor
     */
    public ThrottledRunnableExecutor( 
            final int delayInMillis,
            final ThrottledRunnableAggregator< T > delayedRunnableAggregator )
    {
        this( delayInMillis, delayedRunnableAggregator, WhenAggregating.EXECUTE_ON_TIME );
    }
    
    
    /**
     * @param delayInMillis - Required, a Runnable will be delayed from executing for this many
     * millis at a time until there were no new Runnables that came in since the last delay
     * 
     * @param delayedRunnableAggregator - Optional, if not provided, Runnables that come in
     * while there is a scheduled-to-run Runnable will be dropped on the floor
     * 
     * @param aggregationExecutionMode - Required, defines the mode of execution when aggregating 
     * runnables
     */
    public ThrottledRunnableExecutor( 
            final int delayInMillis,
            final ThrottledRunnableAggregator< T > delayedRunnableAggregator,
            final WhenAggregating aggregationExecutionMode )
    {
        Validations.verifyNotNull( "Aggregation execution mode", aggregationExecutionMode );
        m_aggregator = delayedRunnableAggregator;
        m_aggregationExecutionMode = aggregationExecutionMode;

        
        setDelayInMillis( delayInMillis );
    }
    
    
    /**
     * Determine whether or not execution is delayed when aggregating (if a runnable has been scheduled to
     * run and before it runs, another runnable is added, the two runnables must be aggregated together).
     */
    public enum WhenAggregating
    {
        /**
         * Ensure that the <code>delayInMillis</code> must elapse as quiet time with no new runnables coming
         * in before executing.  Note that this can result in starvation since there is no guarantee that
         * such a duration of quiet time will elapse.
         */
        DELAY_EXECUTION,
        
        /**
         * Ensure that the <code>delayInMillis</code> is the maximum amount of time that will elapse between
         * when a runnable is added / scheduled and when it runs (unless it is held up by a previous task).  This guarantees no starvation.
         */
        EXECUTE_ON_TIME
    }
    
    
    /**
     * @param delayInMillis - Required, a Runnable will be delayed from executing for this many
     * millis at a time until there were no new Runnables that came in since the last delay
     */
    public void setDelayInMillis( final int delayInMillis )
    {
        if ( 1 > delayInMillis )
        {
            throw new IllegalArgumentException( 
                    "Delay in millis must be a positive integer." ); 
        }
        
        m_delayInMillis = delayInMillis;
    }
    
    
    private final class RunnableCompletionNotifierImpl implements RunnableCompletionNotifier
    {
        @Override
        synchronized public void completed()
        {
            if ( m_sentNotification )
            {
                throw new IllegalStateException(
                        "Already sent completed notification." ); 
            }
            
            m_sentNotification = true;
            m_currentlyExecutingThread = null;
        }
        
        private boolean m_sentNotification;
    } // end inner class def
    
    
    /**
     * @return true if there are no runnables currently executing or scheduled
     */
    public boolean isIdle()
    {
        return ( null == m_delayedRunnable && ( null == m_currentlyExecutingThread ) );
    }
    
    
    /**
     * @return true if a runnable is being actively executed
     */
    public boolean isExecutingRunnable()
    {
        return ( null != m_currentlyExecutingThread );
    }
    
    
    /**
     * @return true if there is a delayed runnable scheduled to run in the future
     */
    public boolean isDelayedRunnableScheduledToRunInTheFuture()
    {
        return ( null != m_delayedRunnable );
    }
    
    
    public void add( final T delayedRunnable )
    {
        if ( null == delayedRunnable )
        {
            throw new IllegalArgumentException( 
                    "Delayed runnable cannot be null." ); 
        }
        
        synchronized ( m_lock )
        {
            if ( null == m_delayedRunnable )
            {
                schedule( delayedRunnable );
            }
            else
            {
                aggregate( delayedRunnable );
            }
        }
    }
    
    
    private void schedule( final T delayedRunnable )
    {
        m_runnablesCameInRecently = false;
        m_delayedRunnable = delayedRunnable;
        try
        {
            m_timer = new Timer( new StringBuilder( 100 )
                .append( this.getClass().getSimpleName() )
                .append( '-' )
                .append( delayedRunnable.getClass().getSimpleName() )
                .toString(), true );
        }
        catch ( final Throwable ex )
        {
            HeapDumper.dumpAndZipHeapDueToError();
            throw new RuntimeException( ex );
        }
        m_timer.scheduleAtFixedRate( 
                new DelayedRunnableRunner(), 
                m_delayInMillis,
                m_delayInMillis );
    }
    
    
    private void aggregate( final T delayedRunnable )
    {
        if ( WhenAggregating.DELAY_EXECUTION == m_aggregationExecutionMode )
        {
            m_runnablesCameInRecently = true;
        }
        if ( null == m_aggregator )
        {
            return;
        }
        
        m_aggregator.aggregate( m_delayedRunnable, delayedRunnable );
    }
    
    
    private final class DelayedRunnableRunner extends TimerTask
    {
        @Override
        public void run()
        {
            final ThrottledRunnable runnableToExecute;
            synchronized ( m_lock )
            {
                final Thread currentlyExecutingThread = m_currentlyExecutingThread;
                if ( m_runnablesCameInRecently || ( null != currentlyExecutingThread ) )
                {
                    m_runnablesCameInRecently = false;
                    if ( ( null != currentlyExecutingThread ) 
                            && 15 < m_durationSinceLastLogged.getElapsedSeconds()  )
                    {
                        m_durationSinceLastLogged.reset();
                        LOG.info( new StringBuilder( 200 )
                            .append( this.getClass().getSimpleName() )
                            .append( " waiting to execute " ) 
                            .append( m_delayedRunnable.getClass().getName() )
                            .append( " since the previous runnable has been " ) 
                            .append( "executing for " ) 
                            .append( m_executionDuration )
                            .append( ":" )
                            .append( ExceptionUtil.getLimitedStackTrace( 
                                    currentlyExecutingThread.getStackTrace(), 16 ) )
                            .toString() );
                    }
                    return; //unless an aggregator was defined, we are dropping this new scheduled task on the floor
                }
                runnableToExecute = m_delayedRunnable;
                m_timer.cancel();
                m_timer = null;
                m_currentlyExecutingThread = Thread.currentThread();
                m_durationSinceLastLogged.reset();
                m_delayedRunnable = null;
            }
            
            m_executionDuration.reset();
            try
            {
                runnableToExecute.run( new RunnableCompletionNotifierImpl() );
            }
            catch ( final Exception ex )
            {
                LOG.error( "Failed to run " + runnableToExecute.getClass().getName() + ".", ex );
            }
        }
    } // end inner class def
    
    
    private volatile Thread m_currentlyExecutingThread;
    private volatile T m_delayedRunnable;
    
    private Timer m_timer;
    private boolean m_runnablesCameInRecently;
    
    private volatile int m_delayInMillis;
    private final ThrottledRunnableAggregator< T > m_aggregator;
    private final Object m_lock = new Object();
    private final Duration m_durationSinceLastLogged = new Duration();
    private final Duration m_executionDuration = new Duration();
    private final WhenAggregating m_aggregationExecutionMode;
    
    private final static Logger LOG = Logger.getLogger( ThrottledRunnableExecutor.class );
}
