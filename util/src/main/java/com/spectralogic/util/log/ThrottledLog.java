/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.log;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

/**
 * Ensures that log statements made to the backing log are throttled to not occur more frequently than the
 * configured interval.
 */
public final class ThrottledLog
{
    public ThrottledLog( final Logger log, final int maxLogIntervalInMillis )
    {
        m_log = log;
        m_maxLogIntervalInMillis = maxLogIntervalInMillis;
        Validations.verifyNotNull( "Log", log );
    }
    
    
    synchronized public void info( final Object message )
    {
        if ( isConsumed( 1 ) )
        {
            return;
        }
        
        m_log.log( Level.INFO, message );
    }
    
    
    synchronized public void info( final Object message, final Throwable t )
    {
        if ( isConsumed( 1 ) )
        {
            return;
        }

        m_log.log( Level.INFO, message, t );
    }
    
    
    synchronized public void warn( final Object message )
    {
        if ( isConsumed( 2 ) )
        {
            return;
        }

        m_log.log( Level.WARN, message );
    }
    
    
    synchronized public void warn( final Object message, final Throwable t )
    {
        if ( isConsumed( 2 ) )
        {
            return;
        }

        m_log.log( Level.WARN, message, t );
    }
    
    
    synchronized public void error( final Object message )
    {
        if ( isConsumed( 3 ) )
        {
            return;
        }

        m_log.log( Level.ERROR, message );
    }
    
    
    synchronized public void error( final Object message, final Throwable t )
    {
        if ( isConsumed( 3 ) )
        {
            return;
        }

        m_log.log( Level.ERROR, message, t );
    }
    
    
    private boolean isConsumed( final int logLevel )
    {
        if ( logLevel <= m_lastLogLevel && null != m_durationSinceLastLog
                && m_durationSinceLastLog.getElapsedMillis() < m_maxLogIntervalInMillis )
        {
            return true;
        }
        
        m_lastLogLevel = logLevel;
        m_durationSinceLastLog = new Duration();
        return false;
    }
    
    
    private Duration m_durationSinceLastLog = null;
    private int m_lastLogLevel;
    
    private final Logger m_log;
    private final int m_maxLogIntervalInMillis;
}
