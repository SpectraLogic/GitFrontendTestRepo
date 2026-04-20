/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface TapeFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< TapeFailureNotificationPayload >
{
    String TAPE_ID = "tapeId";
    
    UUID getTapeId();
    
    TapeFailureNotificationPayload setTapeId( final UUID value );
    
    
    String TAPE_DRIVE_ID = "tapeDriveId";

    UUID getTapeDriveId();
    
    TapeFailureNotificationPayload setTapeDriveId( final UUID value );
    
    
    String DATE = "date";

    Date getDate();
    
    TapeFailureNotificationPayload setDate( final Date value );
    
    
    String TYPE = "type";
    
    TapeFailureType getType();
    
    TapeFailureNotificationPayload setType( final TapeFailureType value );
}
