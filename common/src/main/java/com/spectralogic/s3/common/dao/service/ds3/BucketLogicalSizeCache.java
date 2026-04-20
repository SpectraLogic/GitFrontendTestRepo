/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

public interface BucketLogicalSizeCache
{
    void blobCreated( final UUID bucketId, final long sizeInBytes );
    
    
    void blobDeleted( final UUID bucketId, final long sizeInBytes );
    
    
    void bucketDeleted( final UUID bucketId );
    
    
    long getSize( final UUID bucketId );
}
