/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain;

import java.util.List;


public interface NotificationEvent< R >
{
    List< R > getNotificationReceivers();
    
    
    NotificationPayloadGenerator getEventGenerator();
}
