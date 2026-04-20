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
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface TapePartitionFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< TapePartitionFailureNotificationPayload >
{
    String PARTITION_ID = "partitionId";
    
    UUID getPartitionId();
    
    TapePartitionFailureNotificationPayload setPartitionId( final UUID value );
    
    
    String DATE = "date";

    Date getDate();
    
    TapePartitionFailureNotificationPayload setDate( final Date value );
    
    
    String TYPE = "type";
    
    TapePartitionFailureType getType();
    
    TapePartitionFailureNotificationPayload setType( final TapePartitionFailureType value );
}
