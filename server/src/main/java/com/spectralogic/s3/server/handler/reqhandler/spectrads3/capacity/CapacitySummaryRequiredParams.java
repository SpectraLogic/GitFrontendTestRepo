/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.capacity;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.lang.References;

public interface CapacitySummaryRequiredParams extends SimpleBeanSafeToProxy
{
    String STORAGE_DOMAIN_ID = "storageDomainId";
    
    @References( StorageDomain.class )
    UUID getStorageDomainId();
    
    CapacitySummaryRequiredParams setStorageDomainId( final UUID value );
    
    
    String BUCKET_ID = "bucketId";
    
    @References( Bucket.class )
    UUID getBucketId();
    
    CapacitySummaryRequiredParams setBucketId( final UUID bucketId );
}
