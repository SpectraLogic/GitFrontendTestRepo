/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.spectralogic.util.io.lang.HeapDumper;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.StandardShutdownListener;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class RecurringRunnableExecutor extends BaseShutdownable
{
    public RecurringRunnableExecutor( final Runnable runnable, final long intervalInMillis )
    {
        m_runnable = runnable;
        m_intervalInMillis = intervalInMillis;
        
        Validations.verifyNotNull( "Runnable", m_runnable );
        Validations.verifyInRange( "Interval", 1, Integer.MAX_VALUE, intervalInMillis );
        
        final String timerName = m_runnable.getClass().getSimpleName();
        synchronized ( TIMER_NUMBERS )
        {
            if ( !TIMER_NUMBERS.containsKey( timerName ) )
            {
                TIMER_NUMBERS.put( timerName, Integer.valueOf( 1 ) );
            }
            final int timerNumber = TIMER_NUMBERS.get( timerName ).intValue();
            TIMER_NUMBERS.put( timerName, Integer.valueOf( timerNumber + 1 ) );
            m_timerName = m_runnable.getClass().getSimpleName() 
                          + ( ( 1 == timerNumber ) ? "" : "-" + timerNumber );
        }
        addShutdownListener( new CleanupOnShutdown() );
    }
    
    
    private final class CleanupOnShutdown extends StandardShutdownListener
    {
        public void shutdownOccurred()
        {
            synchronized ( RecurringRunnableExecutor.this )
            {
                if ( null == m_timer )
                {
                    return;
                }
                
                m_timer.cancel();
                m_timer = null;
            }
        }
    } // end inner class def
    
    
    synchronized public RecurringRunnableExecutor start()
    {
        verifyNotShutdown();
        if ( null != m_timer )
        {
            return this;
        }
        
        try
        {
            m_timer = new MonitoredJavaTimer( m_timerName, true );
        }
        catch ( final Throwable ex )
        {
            HeapDumper.dumpAndZipHeapDueToError();
            throw new RuntimeException( ex );
        }
        
        m_timer.scheduleAtFixedRate( 
                new RecurringTimerTask( this ),
                m_intervalInMillis, 
                m_intervalInMillis );
        LOG.info( "Scheduled " + m_timerName + " to run every " + m_intervalInMillis + "ms." );
        return this;
    }
    
    
    private final static class RecurringTimerTask extends TimerTask
    {
        private RecurringTimerTask( final RecurringRunnableExecutor executor )
        {
            m_executor = new WeakReference<>( executor );
            m_description =
                    RecurringRunnableExecutor.class.getSimpleName() 
                    + " for " + executor.m_runnable.getClass().getName();
        }
        
        @Override
        public void run()
        {
            final RecurringRunnableExecutor executor = m_executor.get();
            if ( null == executor )
            {
                LOG.error( m_description + " was gc'd without having been properly shut down.  " 
                          + "This is a programming error.  Either call shutdown, " 
                          + "or strongly hold onto the instance to prevent premature gc." );
                cancel();
                return;
            }
            
            final MonitoredWork mw = new MonitoredWork( StackTraceLogging.FULL,
                                                        executor.m_timerName );
            try
            {
                executor.m_runnable.run();
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to run " + executor.m_runnable.getClass().getSimpleName() + ".", ex );
            }
            finally
            {
                mw.completed();
            }
        }
        
        private final WeakReference< RecurringRunnableExecutor > m_executor;
        private final String m_description;
    } // end inner class def
    
    
    private volatile MonitoredJavaTimer m_timer;
    
    private final String m_timerName;
    private final Runnable m_runnable;
    private final long m_intervalInMillis;
    
    private final static Map< String, Integer > TIMER_NUMBERS = new HashMap<>();
    private final static Logger LOG = Logger.getLogger( RecurringRunnableExecutor.class );
}
