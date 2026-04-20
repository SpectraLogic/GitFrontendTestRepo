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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

/**
 * Detects when a thread has consumed the CPU too long in a given time duration.
 */
public final class CpuHogDetector
{
    public CpuHogDetector(
            final int intervalBetweenDetectionCyclesInMillis,
            final int minimumCpuTimeWithinIntervalInMillisRequiredToBeConsideredCpuHog )
    {
        m_minimumCpuTimeWithinIntervalInMillisRequiredToBeConsideredCpuHog =
            minimumCpuTimeWithinIntervalInMillisRequiredToBeConsideredCpuHog;
        m_jvmThreadSystem = ManagementFactory.getThreadMXBean();
        if (!m_jvmThreadSystem.isThreadContentionMonitoringEnabled()) {
            LOG.warn("Thread contention monitoring is not enabled on this system.");
        } else {
            LOG.info("Thread contention monitoring is enabled on this system.");
        }
        if (!m_jvmThreadSystem.isThreadCpuTimeSupported()) {
            LOG.warn("Thread CPU time measurement is not supported on this system.");
        } else {
            m_timer.scheduleAtFixedRate(
                    new CpuHogDetectionTimerTask(),
                    intervalBetweenDetectionCyclesInMillis,
                    intervalBetweenDetectionCyclesInMillis);
            LOG.info(this.getClass().getSimpleName()
                    + " started and will check for cpu hogs every "
                    + intervalBetweenDetectionCyclesInMillis + "ms.");
        }
    }
    
    
    private final class CpuHogDetectionTimerTask extends TimerTask
    {
        @Override
        public void run()
        {
            try
            {
                final Duration duration = new Duration();
                runInternal();
                final long durationLength = duration.getElapsedMillis();
                if ( 100 < durationLength )
                {
                    LOG.warn( this.getClass().getSimpleName() + " took "
                        + durationLength
                        + "ms to run.  Note that this is not consumed CPU"
                        + " time, but real time to execute a hog detection "
                        + "iteration.  That something so quick should take "
                        + "so long suggests the system has "
                        + "become unresponsive." );
                }
            }
            catch ( final Exception ex )
            {
                LOG.error( this.getClass().getName() + " failed to execute successfully.", ex );
            }
        }
        
        private void runInternal()
        {
            final Map< ThreadInfo, Integer > cpuHogs = new HashMap<>();
            for ( final long threadId : m_jvmThreadSystem.getAllThreadIds() )
            {
                final Long tid = Long.valueOf( threadId );
                final long nanosecondsExecutionTime = m_jvmThreadSystem.getThreadCpuTime( threadId );
                final Integer millisecondsExecutionTime = 
                        Integer.valueOf( (int)( nanosecondsExecutionTime / (1000 * 1000) ) );
                final Integer initialExecutionTime = m_initialThreadExecutionTime.get( tid );
                m_initialThreadExecutionTime.put( tid, millisecondsExecutionTime );
                
                if ( null == initialExecutionTime )
                {
                    continue;
                }
                final int durationInMillis = 
                        millisecondsExecutionTime.intValue() - initialExecutionTime.intValue();
                if ( m_minimumCpuTimeWithinIntervalInMillisRequiredToBeConsideredCpuHog > durationInMillis )
                {
                    continue;
                }
                
                cpuHogs.put(
                        m_jvmThreadSystem.getThreadInfo( threadId ), 
                        Integer.valueOf( durationInMillis ) );
            }
            
            if ( cpuHogs.isEmpty() )
            {
                return;
            }
            LOG.warn( new StringBuilder( 200 )
                .append( this.getClass().getSimpleName() )
                .append( " detected " ) 
                .append( cpuHogs.size() )
                .append( " potential cpu hogs.  Notifying listeners." ) 
                .toString() );
            for ( final CpuHogListener listener : m_listeners )
            {
                listener.cpuHogOccurred( new HashMap<>( cpuHogs ) );
            }
        }
    } // end inner class def
    
    
    public void addCpuHogListener( final CpuHogListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        m_listeners.add( listener );
    }
    
    private final Map< Long, Integer > m_initialThreadExecutionTime = new HashMap<>();
    private final int m_minimumCpuTimeWithinIntervalInMillisRequiredToBeConsideredCpuHog;
    private final ThreadMXBean m_jvmThreadSystem;
    private final List< CpuHogListener > m_listeners = new CopyOnWriteArrayList<>();
    private final Timer m_timer = new Timer( this.getClass().getSimpleName(), true );
    
    private final static Logger LOG = Logger.getLogger( CpuHogDetector.class );
}
