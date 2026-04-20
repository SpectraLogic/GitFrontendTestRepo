/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.platform.cache;

import java.util.UUID;

import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;

public interface DiskManager extends CacheManager
{   
    /**
     * @return the file for the blob if it is both allocated and fully loaded in the cache, or if it is available
     * on pool; else, throws an exception
     */
    DiskFileInfo getDiskFileFor(final UUID blobId );
        
    
    /**
     * @return true if the blob is both allocated and fully loaded in the cache, or is available on pool
     */
    boolean isOnDisk( final UUID blobId );
    

    /**
     * @return true if every blob in the specified chunk is available on pool, OR is (i) allocated,
     * (ii) fully loaded in the cache, and (iii) has a checksum in the database
     */
    boolean isChunkEntirelyOnDisk( final UUID chunkId );
    
    
    void processDeleteResults( final S3ObjectService.DeleteResult deleteResult );
    
    
    void deleteBucket( final UUID bucketId, final String bucketName );
}
