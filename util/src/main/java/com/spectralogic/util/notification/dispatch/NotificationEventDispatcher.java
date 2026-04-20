/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.dispatch;

import com.spectralogic.util.notification.domain.NotificationEvent;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public interface NotificationEventDispatcher
{
    void fire( final NotificationEvent< ? > event );


    void queueFire( final NotificationEvent< ? > event );


    NotificationEventDispatcher startTransaction();
    
    
    void commitTransaction();
    
    
    void registerListener( final NotificationListener listener,
            final Class<? extends HttpNotificationRegistration< ? > > notificationRegistrationType );
            
    void unregisterListener( final NotificationListener listener,
            final Class<? extends HttpNotificationRegistration< ? > > notificationRegistrationType );
}
