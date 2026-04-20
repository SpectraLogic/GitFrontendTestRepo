/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain;


import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public interface NotificationPayloadGenerator
{
    NotificationPayload generateNotificationPayload();
}

