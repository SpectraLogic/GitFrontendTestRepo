/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.util.Arrays;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;


/** */
public final class MonitoredJavaTimer extends Timer
{

    public MonitoredJavaTimer()
    {
        super();
    }
    
    
    public MonitoredJavaTimer( final String timerName, final boolean b )
    {
        super( timerName, b );
    }


    @Override
    public void cancel()
    {
        super.cancel();
        LOG.warn( "Java Timer \"" + m_timerName + "\" has been canceled." +
                  /* Next sub-string is used in a test method of this
                     class; update the test if you change it. */
                  " It is dead and none of its managed tasks will run" +
                  " again, nor can new tasks be added to it. Stack trace of" +
                  " thread it was canceled on: " + Platform.NEWLINE +
                    Arrays.toString( Thread.currentThread().getStackTrace() ) );
    }

    
    @Override
    public void scheduleAtFixedRate(
                     final TimerTask task, final long delay, final long period )
    {
        setWorkerThreadDeathHandler();
        final TimerTask wrappedTask = new TimerTask() {
			@Override
			public void run()
			{
				m_timerName = Thread.currentThread().getName();
	            Thread.currentThread().setUncaughtExceptionHandler( m_thrdDeathHandler );
				task.run();
			}
		};
        super.scheduleAtFixedRate( wrappedTask, delay, period );
    }

    
    @Override
    public void scheduleAtFixedRate(
                 final TimerTask task, final Date firstTime, final long period )
    {
        setWorkerThreadDeathHandler();
        final TimerTask wrappedTask = new TimerTask() {
			@Override
			public void run()
			{
				m_timerName = Thread.currentThread().getName();
	            Thread.currentThread().setUncaughtExceptionHandler( m_thrdDeathHandler );
				task.run();
			}
		};
        super.scheduleAtFixedRate( wrappedTask, firstTime, period );
    }

    
    private synchronized void setWorkerThreadDeathHandler()
    {
        if ( null != m_thrdDeathHandler )
        {
            return;
        }
        m_thrdDeathHandler = new ThreadDeathHandler();
    }
    
    
    private final class ThreadDeathHandler implements Thread.UncaughtExceptionHandler
    {
        @Override
        public void uncaughtException( final Thread t, final Throwable e )
        {
            LOG.error( "Work thread of Java Timer \"" + m_timerName +
                       /* Next sub-string is used in a test method of this
                          class; update the test if you change it. */
                       "\" experienced an uncaught exception. Its work" +
                       " thread's name at time of death was \"" + t.getName() +
                       "\". This Timer instance is dead, and none of its" +
                       " managed tasks will run again.", e );
        }
    }

    
    private ThreadDeathHandler m_thrdDeathHandler = null;
    private String m_timerName = null;
    
    private static final Logger LOG = Logger.getLogger( MonitoredJavaTimer.class );
    
}
