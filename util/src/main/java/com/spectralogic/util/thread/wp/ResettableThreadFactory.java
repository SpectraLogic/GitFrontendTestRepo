/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import java.util.concurrent.ThreadFactory;

import com.spectralogic.util.io.lang.HeapDumper;

/**
 * A thread factory that will reset its threads back to their original settings before re-using them.
 */
final class ResettableThreadFactory implements ThreadFactory
{
    ResettableThreadFactory( final boolean daemon, final String basename )
    {
        m_daemon = daemon;
        m_basename = basename;
    }

    
    public boolean isDaemon()
    {
        return m_daemon;
    }

    
    synchronized public Thread newThread( final Runnable r )
    {
        if ( null == r )
        {
            throw new IllegalArgumentException( "Null Runnable not allowed." ); 
        }

        final StringBuilder builder = new StringBuilder( m_basename.length() + 6 );
        builder.append( m_basename ).append( '-' ).append( m_threadNumber );
        ++m_threadNumber;

        final Thread thread;

        try
        {
            thread = new ResettableThread( r, builder.toString(), m_daemon );
        }
        catch ( final Throwable ex )
        {
            HeapDumper.dumpAndZipHeapDueToError();
            throw new RuntimeException( "Unable to create new thread.", ex ); 
        }

        return thread;
    }
    

    private final boolean m_daemon;
    private int m_threadNumber = 1;
    private final String m_basename;
}
