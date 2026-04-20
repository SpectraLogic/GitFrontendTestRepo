/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface DetailedJobToReplicate extends SimpleBeanSafeToProxy
{
    String JOB = "job";
    
    JobToReplicate getJob();
    
    DetailedJobToReplicate setJob( final JobToReplicate value );
    
    
    String USER_ID = "userId";
    
    UUID getUserId();
    
    DetailedJobToReplicate setUserId( final UUID value );
    
    
    String BUCKET_ID = "bucketId";
    
    UUID getBucketId();
    
    DetailedJobToReplicate setBucketId( final UUID value );
    
    
    String PRIORITY = "priority";
    
    BlobStoreTaskPriority getPriority();
    
    DetailedJobToReplicate setPriority( final BlobStoreTaskPriority value );
    
    
    String VERIFY_AFTER_WRITE = "verifyAfterWrite";
    
    /**
     * @return true if data should be verified after it is written (only applies to PUT jobs)
     */
    boolean isVerifyAfterWrite();
    
    DetailedJobToReplicate setVerifyAfterWrite( final boolean value );
    
    
    String CACHED_SIZE_IN_BYTES = "cachedSizeInBytes";
    
    /*
     * Used only for IOM jobs. Since we create the IOM GET before the IOM PUT, the cahced size of the GET may
     * already be non-zero before the PUT is created if some of the data we need is already in cache. Therefore
     * we must replicate this information to the PUT when it is created.  
     */
    @Optional
    Long getCachedSizeInBytes();
    
    DetailedJobToReplicate setCachedSizeInBytes( final Long value );
}
