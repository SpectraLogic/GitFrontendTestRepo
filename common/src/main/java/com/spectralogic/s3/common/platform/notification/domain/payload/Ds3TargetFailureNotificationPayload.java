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

public interface Ds3TargetFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< Ds3TargetFailureNotificationPayload >
{
    String TARGET_ID = "targetId";

    UUID getTargetId();

    Ds3TargetFailureNotificationPayload setTargetId( final UUID value );


    String DATE = "date";

    Date getDate();

    Ds3TargetFailureNotificationPayload setDate( final Date value );


    String TYPE = "type";

    TargetFailureType getType();

    Ds3TargetFailureNotificationPayload setType( final TargetFailureType value );
}
