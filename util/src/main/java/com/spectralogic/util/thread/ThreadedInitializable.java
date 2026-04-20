/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public abstract class ThreadedInitializable
{
    final protected void startInitialization()
    {
        if ( m_initializationStarted.getAndSet( true ) )
        {
            throw new IllegalStateException( "Initialization already begun." );
        }
        
        LOG.info( getClass().getSimpleName() + " initializing..." );
        final Set< Runnable > initializers = getInitializers();
        m_initializedLatch = new CountDownLatch( initializers.size() );
        for ( final Runnable r : initializers )
        {
            SystemWorkPool.getInstance().submit( new Initializer( r ) );
        }
        SystemWorkPool.getInstance().submit( new InitializationCompletedListener() );
    }
    
    
    private final class Initializer implements Runnable
    {
        private Initializer( final Runnable r )
        {
            m_r = r;
        }
        
        public void run()
        {
            final MonitoredWork work = new MonitoredWork( 
                    StackTraceLogging.SHORT, 
                    "Initialize " + ThreadedInitializable.this.getClass().getSimpleName() + ": " 
                    + m_r.getClass().getSimpleName() );
            try
            {
                m_r.run();
            }
            catch ( final Throwable ex )
            {
                killMethodsAwaitingInitialization(ex);
                throw ex;
            }
            finally
            {
                work.completed();
            }
            
            m_initializedLatch.countDown();
        }
        
        private final Runnable m_r;
    } // end inner class def
    
    
    private final class InitializationCompletedListener implements Runnable
    {
        public void run()
        {
            try
            {
                m_initializedLatch.await();
            }
            catch ( InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }

            if ( m_latestException == null )
            {
                LOG.info( ThreadedInitializable.this.getClass().getSimpleName() 
                        + " initialized in " + m_life + "." );
                m_initialized = true;
            }
            else
            {
                LOG.info( ThreadedInitializable.this.getClass().getSimpleName() + " failed to initialize." );
            }
        }
    } // end inner class def
    
    
    abstract protected Set< Runnable > getInitializers();
    
    
    public void killMethodsAwaitingInitialization( final Throwable ex )
    {
        // It doesn't really matter which initializers failed because we're logging
        // all of the initialization exceptions anyway.
        m_latestException = ex;
        while ( 0 < m_initializedLatch.getCount() )
        {
            m_initializedLatch.countDown();
        }
    }


    public final void waitUntilInitialized()
    {
        if ( m_initialized )
        {
            return;
        }
        
        LOG.warn( getClass().getSimpleName() + " isn't initialized yet.  Will block until initialized." );
        try
        {
            m_initializedLatch.await();
            if ( m_latestException != null )
            {
                throw new IllegalStateException(
                        "Waiting for initializers, but one or more of them failed.",
                        m_latestException );
            }
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private volatile boolean m_initialized;
    private volatile CountDownLatch m_initializedLatch;
    private volatile Throwable m_latestException = null;
    private final AtomicBoolean m_initializationStarted = new AtomicBoolean( false );
    private final Duration m_life = new Duration();
    
    private final static Logger LOG = Logger.getLogger( ThreadedInitializable.class );
}
