/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface AzureTargetFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< AzureTargetFailureNotificationPayload >
{
    String TARGET_ID = "targetId";

    UUID getTargetId();

    AzureTargetFailureNotificationPayload setTargetId( final UUID value );


    String DATE = "date";

    Date getDate();

    AzureTargetFailureNotificationPayload setDate( final Date value );


    String TYPE = "type";

    TargetFailureType getType();

    AzureTargetFailureNotificationPayload setType( final TargetFailureType value );
}
