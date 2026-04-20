/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface PoolFailureNotificationPayload
    extends NotificationPayload, ErrorMessageObservable< PoolFailureNotificationPayload >
{
    String POOL_ID = "poolId";
    
    UUID getPoolId();
    
    PoolFailureNotificationPayload setPoolId( final UUID value );
    
    
    String DATE = "date";

    Date getDate();
    
    PoolFailureNotificationPayload setDate( final Date value );
    
    
    String TYPE = "type";
    
    PoolFailureType getType();
    
    PoolFailureNotificationPayload setType( final PoolFailureType value );
}
