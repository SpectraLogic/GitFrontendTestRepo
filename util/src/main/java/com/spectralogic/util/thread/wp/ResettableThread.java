/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;


/**
 * Thread that can be reset so that thread characteristics (such as priority and name) that are changed by a
 * {@link Runnable} using that thread can be reset to the original settings before the thread is re-used by
 * another {@link Runnable}.
 */
final class ResettableThread extends Thread
{
    ResettableThread( final Runnable r, final String threadName, final boolean runAsDaemon )
    {
        super( r, threadName );
        m_threadName = threadName;
        m_runAsDaemon = runAsDaemon;

        reset();
    }

    
    void reset()
    {
        setName( m_threadName );

        if ( Thread.NORM_PRIORITY != getPriority() )
        {
            setPriority( Thread.NORM_PRIORITY );
        }
        if ( m_runAsDaemon != isDaemon() )
        {
            setDaemon( m_runAsDaemon );
        }
        if ( getUncaughtExceptionHandler() != null )
        {
            setUncaughtExceptionHandler( null );
        }
    }
    
    
    /**
     * This is here to satisfy CodePro.
     */
    @Override
    public void run()
    {
        super.run();
    }
    

    private final boolean m_runAsDaemon;
    private final String m_threadName;
}