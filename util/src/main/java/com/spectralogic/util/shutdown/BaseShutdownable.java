/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.shutdown;

import java.util.List;


public abstract class BaseShutdownable implements Shutdownable
{
    final public boolean isShutdown()
    {
        return m_support.isShutdown();
    }
    
    
    public void shutdown()
    {
        m_support.shutdown();
    }
    
    
    final public void verifyNotShutdown()
    {
        m_support.verifyNotShutdown();
    }
    
    
    final public void doNotLogWhenShutdown()
    {
        m_support.doNotLogWhenShutdown();
    }
    
    
    final public void addShutdownListener( final ShutdownListener listener )
    {
        m_support.addShutdownListener( listener );
    }
    
    
    final public List< ShutdownListener > getShutdownListeners()
    {
        return m_support.getShutdownListeners();
    }
    
    
    final public void shutdownOccurred()
    {
        m_support.shutdownOccurred();
    }
    
    
    final public boolean isShutdownListenerNotificationCritical()
    {
        return m_support.isShutdownListenerNotificationCritical();
    }
    
    
    private final ShutdownSupport m_support = new ShutdownSupport( getClass() );
}
