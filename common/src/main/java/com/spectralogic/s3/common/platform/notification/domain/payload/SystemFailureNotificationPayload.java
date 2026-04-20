/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface SystemFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< SystemFailureNotificationPayload >
{
    String DATE = "date";
    
    Date getDate();
    
    SystemFailureNotificationPayload setDate( final Date value );
    
    
    String TYPE = "type";
    
    SystemFailureType getType();
    
    SystemFailureNotificationPayload setType( final SystemFailureType value );
}
