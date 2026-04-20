/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.dispatch;

import java.util.ArrayList;
import java.util.List;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationEvent;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;
import com.spectralogic.util.shutdown.BaseShutdownable;

public final class TransactionNotificationEventDispatcher
    extends BaseShutdownable
    implements NotificationEventDispatcher
{
    public TransactionNotificationEventDispatcher( 
            final NotificationEventDispatcher decoratedDispatcher )
    {
        m_decoratedDispatcher = decoratedDispatcher;
        Validations.verifyNotNull( "Decorated dispatcher", m_decoratedDispatcher );
        doNotLogWhenShutdown();
    }


    public void queueFire( final NotificationEvent< ? > event ) {
        verifyNotShutdown();
        Validations.verifyNotNull( "Event", event );
        m_queuedEvents.add( event );
    }


    synchronized public void fire( final NotificationEvent< ? > event )
    {
        verifyNotShutdown();
        Validations.verifyNotNull( "Event", event );
        m_events.add( event );
    }


    public NotificationEventDispatcher startTransaction()
    {
        throw new IllegalStateException( "Cannot start transaction on a transaction." );
    }


    synchronized public void commitTransaction()
    {
        shutdown();
        for ( final NotificationEvent< ? > event : m_events )
        {
            m_decoratedDispatcher.fire( event );
        }
        for ( final NotificationEvent< ? > event : m_queuedEvents )
        {
            m_decoratedDispatcher.queueFire( event );
        }
    }
    
    
    public void registerListener( final NotificationListener listener,
            final Class<? extends HttpNotificationRegistration< ? > > notificationRegistrationType )
    {
        m_decoratedDispatcher.registerListener( listener, notificationRegistrationType );
    }
    
    
    public void unregisterListener( final NotificationListener listener,
            final Class<? extends HttpNotificationRegistration< ? > > notificationRegistrationType )
    {
        m_decoratedDispatcher.unregisterListener( listener, notificationRegistrationType );
    }


    private final List< NotificationEvent< ? > > m_queuedEvents = new ArrayList<>();
    private final List< NotificationEvent< ? > > m_events = new ArrayList<>();
    private final NotificationEventDispatcher m_decoratedDispatcher;
}
