/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.shutdown;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;

public final class ShutdownSupport implements ShutdownListener
{
    public ShutdownSupport( final Class< ? > thingThatCanBeShutDown )
    {
        this( thingThatCanBeShutDown.getSimpleName() );
    }
    
    
    public ShutdownSupport( final String nameOfTheThingThatCanBeShutDown )
    {
        m_shutdownableName = nameOfTheThingThatCanBeShutDown;
        Validations.verifyNotNull( "Name of the thing that can be shut down", m_shutdownableName );
    }
    
    
    public boolean isShutdown()
    {
        return m_shutdown.get();
    }
    
    
    public void verifyNotShutdown()
    {
        if ( isShutdown() )
        {
            throw new IllegalStateException( m_shutdownableName + " has been shut down." );
        }
    }
    
    
    public ShutdownSupport doNotLogWhenShutdown()
    {
        m_doNotLogWhenShutdown = true;
        return this;
    }
    
    
    public void shutdown()
    {
        if ( m_shutdown.getAndSet( true ) )
        {
            LOG.warn( m_shutdownableName
                      + " has already been shut down.  Ignoring duplicate request to shut down." );
            return;
        }

        if ( !m_doNotLogWhenShutdown )
        {
            LOG.info( "Shutting down " + m_shutdownableName + "..." );
        }
        for ( final ShutdownListener listener : m_shutdownListeners )
        {
            try
            {
                listener.shutdownOccurred();
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Shutdown listener '" + listener.getClass().getName() 
                          + "' failed to handle shutdown event.", ex );
            }
        }
        
        if ( !m_doNotLogWhenShutdown )
        {
            LOG.info( m_shutdownableName + " has shut down." );
        }
    }
    
    
    public void addShutdownListener( final ShutdownListener listener )
    {
        Validations.verifyNotNull( "Listener", listener );
        m_shutdownListeners.add( listener );
    }
    
    
    public List< ShutdownListener > getShutdownListeners()
    {
        return new ArrayList<>( m_shutdownListeners );
    }
    
    
    public void shutdownOccurred()
    {
        shutdown();
    }


    public boolean isShutdownListenerNotificationCritical()
    {
        for ( final ShutdownListener listener : m_shutdownListeners )
        {
            if ( listener.isShutdownListenerNotificationCritical() )
            {
                return true;
            }
        }
        
        return false;
    }
    
    
    @Override
    protected void finalize() throws Throwable
    {
        if ( isShutdown() )
        {
            super.finalize();
            return;
        }
        if ( !isShutdownListenerNotificationCritical() )
        {
            super.finalize();
            return;
        }
        
        try
        {
            throw new IllegalStateException( 
                    m_shutdownableName + " is being garbage collected, but it was never shut down.  " 
                    + "Will call the listeners, but be advised that using a finalizer to cleanup "
                    + "is unreliable (finalizers are not reliably called by JVMs).  Unless this message "
                    + "is showing up while running unit/integration JUnit tests, then this is a bug.  "
                    + "Do not ignore it.  You will need to look into how you can reliably call "
                    + "shutdown before this object is gc'd." );
        }
        catch ( final IllegalStateException ex )
        {
            LOG.warn( "Shutdown was never called on " + m_shutdownableName + ", but it should have been.",
                      ex );
            LOG.warn( "We initialized the offending " + m_shutdownableName + " instance in the stacktrace" +
                      " below.", m_initTrackingException );
        }
        
        shutdown();
        
        super.finalize();
    }
    
    
    private volatile boolean m_doNotLogWhenShutdown;
    private final Exception m_initTrackingException = new Exception( "Missing shutodwn" );
    private final String m_shutdownableName;
    private final AtomicBoolean m_shutdown = new AtomicBoolean();
    private final List< ShutdownListener > m_shutdownListeners = new CopyOnWriteArrayList<>();
    private final static Logger LOG = Logger.getLogger( ShutdownSupport.class );
}
