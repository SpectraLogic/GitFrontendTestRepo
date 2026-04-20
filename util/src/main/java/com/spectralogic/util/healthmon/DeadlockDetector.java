/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

/**
 * Detects deadlocks between threads.
 */
public final class DeadlockDetector
{
    public DeadlockDetector(
            final int intervalBetweenDetectionCyclesInMillis )
    {
        m_timer.scheduleAtFixedRate( 
                new DeadlockDetectionTimerTask(),
                intervalBetweenDetectionCyclesInMillis, 
                intervalBetweenDetectionCyclesInMillis );
        LOG.info( this.getClass().getSimpleName() 
                + " started and will check for deadlock every " 
                + intervalBetweenDetectionCyclesInMillis + "ms." );
    }
    
    
    private final class DeadlockDetectionTimerTask extends TimerTask
    {
        @Override
        public void run()
        {
            verifyNotInterrupted();
            if ( 0 <= --m_numberOfCyclesToSkipDueToDeadlock )
            {
                LOG.warn( this.getClass().getSimpleName()
                        + " skipping this deadlock detection cycle "
                        + "since there was a recent detected deadlock."
                        + "  Will skip the next "
                        + m_numberOfCyclesToSkipDueToDeadlock
                        + " cycles." );
                return;
            }
            
            try
            {
                final Duration duration = new Duration();
                runInternal();
                final long durationLength = duration.getElapsedMillis();
                if ( 100 < durationLength )
                {
                    LOG.warn( this.getClass().getSimpleName()
                            + " took "
                            + durationLength
                            + "ms to run.  This seems a bit excessive." );
                }
            }
            catch ( final Exception ex )
            {
                LOG.error( this.getClass().getName() + " failed to execute successfully.", ex );
            }
            verifyNotInterrupted();
        }
        
        private void runInternal()
        {
            verifyNotInterrupted();
            final Set< ThreadInfo > deadlockedThreads = getDeadlockedThreads();
            if ( deadlockedThreads.isEmpty() )
            {
                m_numberOfCyclesToSkipDueToDeadlock = 0;
                return;
            }
            
            m_numberOfCyclesToSkipDueToDeadlock = 11;
            LOG.info( this.getClass().getSimpleName()
                + " detected a deadlock between " 
                + deadlockedThreads.size()
                + " threads.  Notifying listeners." );
            for ( final DeadlockListener listener : m_listeners )
            {
                listener.deadlockOccurred( new HashSet<>( deadlockedThreads ) );
            }
            verifyNotInterrupted();
        }
        
        private void verifyNotInterrupted()
        {
            if ( Thread.currentThread().isInterrupted() )
            {
                this.cancel();
                throw new RuntimeException( "Interrupted." );
            }
        }
        
        private volatile int m_numberOfCyclesToSkipDueToDeadlock;
    } // end inner class def
    
    
    public static Set< ThreadInfo > getDeadlockedThreads()
    {
        final long [] deadlockedThreadIds = JVM_THREAD_SUBSYSTEM.findDeadlockedThreads();
        if ( null == deadlockedThreadIds || 0 == deadlockedThreadIds.length )
        {
            return new HashSet<>();
        }
        
        return CollectionFactory.toSet( JVM_THREAD_SUBSYSTEM.getThreadInfo( deadlockedThreadIds ) );
    }
    
    
    public static Set< ThreadInfo > getAllThreads()
    {
        return CollectionFactory.toSet( JVM_THREAD_SUBSYSTEM.dumpAllThreads( true, true ) );
    }
    
    
    public void addDeadlockListener( final DeadlockListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        m_listeners.add( listener );
    }
    
    
    private final static ThreadMXBean JVM_THREAD_SUBSYSTEM = ManagementFactory.getThreadMXBean();
    private final List< DeadlockListener > m_listeners = new CopyOnWriteArrayList<>();
    private final Timer m_timer = new Timer( this.getClass().getSimpleName(), true );
    
    private final static Logger LOG = Logger.getLogger( DeadlockDetector.class );
}
