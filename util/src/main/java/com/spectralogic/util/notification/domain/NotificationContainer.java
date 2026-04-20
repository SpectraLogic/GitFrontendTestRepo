/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface NotificationContainer extends SimpleBeanSafeToProxy
{
    String NOTIFICATION = "notification";
    
    Notification getNotification();
    
    void setNotification( final Notification value );
}
