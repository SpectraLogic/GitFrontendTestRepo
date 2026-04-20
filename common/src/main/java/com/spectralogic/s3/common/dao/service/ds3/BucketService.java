/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface BucketService 
    extends BeansRetriever< Bucket >, BeanCreator< Bucket >, BeanUpdater< Bucket >, BeanDeleter
{
    public long getLogicalCapacity( final UUID bucketId );
    
    
    public long getPendingPutWorkInBytes(final UUID bucketId, UUID m_storageDomainId);
    
    
    ReentrantReadWriteLock getLock();
    
    
    BucketLogicalSizeCache getLogicalSizeCache();
    
    
    void initializeLogicalSizeCache();
    
    
    void initializeLogicalSizeCache( final BucketService source );

    public static long DEFAULT_PREFFERRED_CHUNK_SIZE = 64 * 1024 * 1024 * 1024L; //64 GiB
}
