/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.thread.workmon;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

public final class MonitoredWorkManager
{
    private MonitoredWorkManager()
    {
        m_executor.start();
    }
    
    
    public void submit( final MonitoredWork work )
    {
        Validations.verifyNotNull( "Work", work );
        synchronized ( m_work )
        {
            m_work.add( work );
        }
        work.configureWorkSet( m_work );
    }
    
    
    public static MonitoredWorkManager getInstance()
    {
        return INSTANCE;
    }
    
    
    private final class WorkLogger implements Runnable
    {
        public void run()
        {
            final Set< MonitoredWork > works;
            synchronized ( m_work )
            {
                works = new HashSet<>( m_work );
            }
            
            final Set< MonitoredWork > deletes = new HashSet<>();
            for ( final MonitoredWork work : works )
            {
                final Duration duration = work.getDuration();
                if ( null == duration )
                {
                    deletes.add( work );
                    continue;
                }
                
                final int secs = duration.getElapsedSeconds();
                if ( 5 < secs )
                {
                    final int logEvery = getLogEvery( secs );
                    final int shouldLogAt = ( secs / logEvery ) * logEvery;
                    if ( work.getLoggedAt() < shouldLogAt )
                    {
                        work.setLoggedAt( shouldLogAt );
                        Logger log = work.getCustomLogger();
                        if ( null == log )
                        {
                            log = LOG;
                        }
                        String message = work.getCustomMessage( duration );
                        if ( null == message )
                        {
                            message = "Still in progress after " + duration;
                        }
                        message += ": " + getLogMessage( work );
                        log.info( message );
                        if ( work.shouldStopMonitoring() && ( ( 24 * 60 ) < duration.getElapsedMinutes() ) )
                        {
                            log.warn( "Stopped monitoring: " + getLogMessage( work ) );
                            work.completed();
                        }
                    }
                }
                
                synchronized ( m_work )
                {
                    m_work.removeAll( deletes );
                }
            }
        }
        
        private String getLogMessage( final MonitoredWork work )
        {
            if ( work.getStackTraceLogging().getMaxDepth() <= 0 )
            {
                return "[" + work.getThread().getName() + "] " + work.getDescription();
            }
            
            return work.getDescription() 
                    + Platform.NEWLINE + "Thread " + work.getThread().getName() + " is " 
                    + work.getThread().getState() + ":"
                    + ExceptionUtil.getLimitedStackTrace( 
                            work.getThread().getStackTrace(), 
                            work.getStackTraceLogging().getMaxDepth() );
        }
        
        private int getLogEvery( final int elapsedSecs )
        {
            if ( 15 > elapsedSecs )
            {
                return 10;
            }
            return 60;
        }
    } // end inner class def
    
    
    private final Set< MonitoredWork > m_work = new HashSet<>();
    private final RecurringRunnableExecutor m_executor = 
            new RecurringRunnableExecutor( new WorkLogger(), 1000 );
    
    private final static MonitoredWorkManager INSTANCE = new MonitoredWorkManager();
    private final static Logger LOG = Logger.getLogger( MonitoredWorkManager.class );
}
