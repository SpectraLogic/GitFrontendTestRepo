/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ThreadInfo;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;

public final class DeadlockListenerImpl implements DeadlockListener
{
    public void deadlockOccurred( final Set< ThreadInfo > deadlockedThreads )
    {
        LOG.error( getLogStatement( deadlockedThreads ) );
    }
    
    
    public String getLogStatement( final Set< ThreadInfo > deadlockedThreads )
    {
        if ( null == deadlockedThreads )
        {
            throw new IllegalArgumentException(
                    "Deadlocked threads cannot be null." ); 
        }
        
        final Map< Thread, StackTraceElement[] > allStackTraces = Thread.getAllStackTraces();
        
        String msg = Platform.NEWLINE
                + Platform.NEWLINE
                + "!!!!!!!!!!!!!! DEADLOCK DETECTED !!!!!!!!!!!!!!" 
                + Platform.NEWLINE
                + this.getClass().getSimpleName()
                + " received notification that a deadlock has occurred " 
                + "between the following " 
                + deadlockedThreads.size()
                + " threads:" 
                + Platform.NEWLINE;
        for ( final ThreadInfo thread : deadlockedThreads )
        {
            msg += Platform.NEWLINE 
                    + thread.toString() 
                    + getStackTraceText( 
                            thread, 
                            getThreadStackTrace( allStackTraces, thread.getThreadId() ) )
                    + Platform.NEWLINE
                    + Platform.NEWLINE
                    + Platform.NEWLINE
                    + Platform.NEWLINE;
        }
        msg += "!!!!!!!!!!!!!! END OF DEADLOCK DETECTED REPORT !!!!!!!!!!!!!!"; 
        msg += Platform.NEWLINE;
        
        return msg;
    }
    
    
    private String getStackTraceText( 
            final ThreadInfo thread,
            final StackTraceElement [] stackTrace )
    {
        if ( null == stackTrace || ( null != thread.getStackTrace() && 0 < thread.getStackTrace().length ) )
        {
            return "";
        }

        final StringBuilder sb = new StringBuilder( 400 );
        for ( int i = 0; i < stackTrace.length; i++ )
        {
            sb.append( Platform.NEWLINE + "        " + stackTrace[ i ].toString() );
        }
        
        return sb.toString();
    }
    
    
    private StackTraceElement [] getThreadStackTrace( 
            final Map< Thread, StackTraceElement [] > allStackTraces,
            final long threadId )
    {
        for ( final Map.Entry< Thread, StackTraceElement [] > e : allStackTraces.entrySet() )
        {
            if ( e.getKey().getId() == threadId )
            {
                return e.getValue();
            }
        }
        
        return null;
    }
    
    
    private final static Logger LOG = Logger.getLogger( DeadlockListenerImpl.class );
}
