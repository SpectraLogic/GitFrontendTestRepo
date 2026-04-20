/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.healthmon;

import java.lang.management.ThreadInfo;
import java.util.Map;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;

public final class CpuHogListenerImpl implements CpuHogListener
{
    public void cpuHogOccurred( final Map< ThreadInfo, Integer > cpuHoggingThreads )
    {
        final Map< Thread, StackTraceElement[] > allStackTraces = Thread.getAllStackTraces();
        final StringBuilder sb = new StringBuilder( 400 )
            .append( Platform.NEWLINE )
            .append( Platform.NEWLINE )
            .append( "!!!!!!!!!!!!!! CPU HOGS DETECTED !!!!!!!!!!!!!!" ) //$NON-NLS-1$
            .append( Platform.NEWLINE )
            .append( this.getClass().getSimpleName() )
            .append( " received notification that " ) //$NON-NLS-1$
            .append( cpuHoggingThreads.size() )
            .append( " cpu hogs exist:" ) //$NON-NLS-1$
            .append( Platform.NEWLINE );
        for ( final Map.Entry< ThreadInfo, Integer > e : cpuHoggingThreads.entrySet() )
        {
            sb.append( Platform.NEWLINE );
            sb.append( "CPU time: " ); //$NON-NLS-1$
            sb.append( e.getValue() ).append( "ms" ); //$NON-NLS-1$
            sb.append( Platform.NEWLINE );
            sb.append( e.getKey().toString() );
            sb.append( getStackTraceText( 
                    e.getKey(), 
                    getThreadStackTrace( allStackTraces, e.getKey().getThreadId() ) ) );
            sb.append( Platform.NEWLINE );
            sb.append( Platform.NEWLINE );
            sb.append( Platform.NEWLINE );
            sb.append( Platform.NEWLINE );
        }
        sb.append( "!!!!!!!!!!!!!! END OF CPU HOGS DETECTED REPORT !!!!!!!!!!!!!!" ); //$NON-NLS-1$
        sb.append( Platform.NEWLINE );
        LOG.warn( sb.toString() );
    }


    private String getStackTraceText( 
            final ThreadInfo thread,
            final StackTraceElement [] stackTrace )
    {
        if ( null == stackTrace || ( null != thread.getStackTrace() && 0 < thread.getStackTrace().length ) )
        {
            return "";
        }

        final String newline = System.getProperty( "line.separator" ); //$NON-NLS-1$
        final StringBuilder sb = new StringBuilder( 400 );
        for ( int i = 0; i < stackTrace.length; i++ )
        {
            sb.append( new StringBuilder( 100 )
                .append( newline )
                .append( "        " ) //$NON-NLS-1$
                .append( stackTrace[ i ].toString() ) );
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
    
    
    private final static Logger LOG = Logger.getLogger( CpuHogListenerImpl.class );
}
