/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface Notification extends SimpleBeanSafeToProxy
{
    String TYPE = "type";
    
    String getType();
    
    Notification setType( final String value );
    
    
    String EVENT = "event";
    
    NotificationPayload getEvent();
    
    Notification setEvent( final NotificationPayload value );
}
