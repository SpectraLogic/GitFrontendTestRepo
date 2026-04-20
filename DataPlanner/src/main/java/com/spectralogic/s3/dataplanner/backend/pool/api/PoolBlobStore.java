/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.api;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.dataplanner.backend.api.LocalBlobStore;

public interface PoolBlobStore extends LocalBlobStore
{
    PoolLockSupport< PoolTask > getLockSupport();
    
    
    /**
     * Queues a compaction for the pool specified, or if null is specified, queues a compaction for every
     * pool in a state where compaction would be possible
     */
    void compactPool( final BlobStoreTaskPriority priority, final UUID poolId );
    
    
    void formatPool( final UUID poolId );
    
    
    void destroyPool( final UUID poolId );
    
    
    void importPool( final BlobStoreTaskPriority priority, final ImportPoolDirective directive );
    
    
    /**
     * Cancels a pending import pool request that has not yet begun.
     * 
     * @return TRUE if the import request for the pool specified could be cancelled
     */
    boolean cancelImportPool( final UUID poolId );
    
    
    /**
     * Cancels a pending verify pool request that has not yet begun.
     * 
     * @return true if a verify was canceled; false if there was no verify scheduled for the pool
     * @throws RuntimeException if the verification of the specified pool could not be canceled
     */
    boolean cancelVerifyPool( final UUID poolId );
    
    
    /**
     * Gets the path for the specified blob ID if it exists.
     *
     * @return the path if available, null otherwise.
     */
    DiskFileInfo getPoolFileFor(final UUID blobId );
    
    
    /**
     * Retrieves a disk manager that serves as an interface for both cache and pool.
     * 
     * @return the disk manager
     */
    DiskManager getDiskManager();
    
    
    void deleteBucket( final UUID bucketId, final String bucketName );
    
    
    void deleteObjects( final String bucketName, final Set< UUID > objectIds );
    
    
    boolean isAvailableOnPool( final UUID blobId );
    
    
    boolean isChunkEntirelyAvailableOnPool( final UUID chunkId );
    
    
    void scheduleBlobReadLockReleaser();


    Object getEnvironmentStateLock();
}
