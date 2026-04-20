/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.thread.workmon;

import java.util.Set;
import java.util.function.Function;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

public final class MonitoredWork
{
    public MonitoredWork( final StackTraceLogging stackTraceLogging, final String description )
    {
        Validations.verifyNotNull( "Stack trace logging", stackTraceLogging );
        Validations.verifyNotNull( "Description", description );
        m_description = description;
        m_thread = Thread.currentThread();
        m_stackTraceLogging = stackTraceLogging;
        MonitoredWorkManager.getInstance().submit( this );
    }
    
    
    public MonitoredWork( final StackTraceLogging stackTraceLogging, final String description,
            final Function< Duration, String > durationFunction )
    {
        this( stackTraceLogging, description );
        m_stringDurationFunction = durationFunction;
    }
    
    
    public String getCustomMessage( final Duration duration )
    {
        return m_stringDurationFunction.apply( duration );
    }
    
    
    public void setCustomMessage( final Function< Duration, String > durationFunction )
    {
        m_stringDurationFunction = durationFunction;
    }
    
    
    public enum StackTraceLogging
    {
        /**
         * Printing the stack trace of the thread is not helpful and shall not be done.
         */
        NONE( -1 ),
        
        /**
         * Printing a shortened version of the stack trace of the thread is helpful and shall be done.
         */
        SHORT( 8 ),
        
        /**
         * Printing a longer version of the stack trace of the thread is helpful and shall be done.
         */
        LONG( 24 ),
        
        /**
         * Printing the full version of the stack trace of the thread is helpful and shall be done.
         */
        FULL( Integer.MAX_VALUE ),
        ;
        
        
        StackTraceLogging( final int maxDepth )
        {
            m_maxDepth = maxDepth;
        }
        
        
        public int getMaxDepth()
        {
            return m_maxDepth;
        }
        
        
        private final int m_maxDepth;
    }
    
    
    public MonitoredWork withCustomLogger( final Logger logger )
    {
        m_logger = logger;
        return this;
    }
    
    
    public Logger getCustomLogger()
    {
        return m_logger;
    }
    
    
    public void completed()
    {
        m_completed = true;
        if ( null != m_work )
        {
            synchronized ( m_work )
            {
                m_work.remove( this );
            }
        }
    }
    
    
    public Duration getDuration()
    {
        if ( m_completed )
        {
            return null;
        }
        return m_duration;
    }
    
    
    public String getDescription()
    {
        return m_description;
    }
    
    
    public Thread getThread()
    {
        return m_thread;
    }
    
    
    public StackTraceLogging getStackTraceLogging()
    {
        return m_stackTraceLogging;
    }
    
    
    public int getLoggedAt()
    {
        return m_loggedAt;
    }
    
    
    public void setLoggedAt( final int value )
    {
        m_loggedAt = value;
    }
    
    
    void configureWorkSet( final Set< MonitoredWork > work )
    {
        m_work = work;
    }
    
    
    public boolean shouldStopMonitoring()
    {
        return m_shouldStopMonitoring;
    }
    
    
    public void setShouldStopMonitoring( final boolean shouldStopMonitoring )
    {
        this.m_shouldStopMonitoring = shouldStopMonitoring;
    }
    
    
    private volatile int m_loggedAt;
    private volatile Logger m_logger;
    private volatile boolean m_completed;
    private volatile Set< MonitoredWork > m_work;
    
    private final StackTraceLogging m_stackTraceLogging;
    private final String m_description;
    private Function< Duration, String > m_stringDurationFunction = x -> null;
    private final Thread m_thread;
    private final Duration m_duration = new Duration();
    private boolean m_shouldStopMonitoring = false;
}
