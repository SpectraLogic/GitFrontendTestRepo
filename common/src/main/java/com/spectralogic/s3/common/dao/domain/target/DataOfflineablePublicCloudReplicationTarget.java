/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import com.spectralogic.util.bean.lang.DefaultIntegerValue;

/**
 * A {@link PublicCloudReplicationTarget} where data can be offlined (for example, residing only on tape or 
 * other offline media), requiring staging before it can be read from the cloud.
 */
public interface DataOfflineablePublicCloudReplicationTarget< T > extends PublicCloudReplicationTarget< T >
{
    String OFFLINE_DATA_STAGING_WINDOW_IN_TB = "offlineDataStagingWindowInTb";
    
    /**
     * @return the working window (in TB) for staging data that is offline to online so that it can be read
     * (cloud providers tend to charge a premium for large working windows and strongly encourage throttled
     * and gradual staging over burst staging)
     */
    @DefaultIntegerValue( 64 )
    int getOfflineDataStagingWindowInTb();
    
    T setOfflineDataStagingWindowInTb( final int value );
    
    
    String STAGED_DATA_EXPIRATION_IN_DAYS = "stagedDataExpirationInDays";
    
    /**
     * @return the number of days offline data that is temporarily brought online (staged) for the purpose
     * of reading it should remain staged and online until it can be expired / made offline again
     */
    @DefaultIntegerValue( 30 )
    int getStagedDataExpirationInDays();
    
    T setStagedDataExpirationInDays( final int value );
}
