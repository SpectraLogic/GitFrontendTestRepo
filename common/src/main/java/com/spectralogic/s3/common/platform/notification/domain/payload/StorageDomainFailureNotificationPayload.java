/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface StorageDomainFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< TapePartitionFailureNotificationPayload >
{
    String STORAGE_DOMAIN_ID = "storageDomainId";
    
    UUID getStorageDomainId();
    
    StorageDomainFailureNotificationPayload setStorageDomainId( final UUID value );
    
    
    String DATE = "date";
    
    Date getDate();
    
    StorageDomainFailureNotificationPayload setDate( final Date value );
    
    
    String TYPE = "type";
    
    StorageDomainFailureType getType();
    
    StorageDomainFailureNotificationPayload setType( final StorageDomainFailureType value );
}
