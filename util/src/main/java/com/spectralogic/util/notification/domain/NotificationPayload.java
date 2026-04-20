/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain;

import java.util.Date;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface NotificationPayload extends SimpleBeanSafeToProxy
{
    String NOTIFICATION_GENERATION_DATE = "notificationGenerationDate";
    
    Date getNotificationGenerationDate();
    
    void setNotificationGenerationDate( final Date value );
}
