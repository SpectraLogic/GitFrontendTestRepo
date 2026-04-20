/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.common.platform.cache;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheInformation;

public interface CacheManager
{
	void allocateChunk(final UUID chunkId );
	
	/**
	 * Allocates a set of chunks. 
	 */
	void allocateChunks(final Set< UUID > chunkIds );

    /**
     * @return the cache file for the blob, allocating the space if and only if necessary
     */
    String allocateChunksForBlob(final UUID blobId );
    
    
    /**
     * @return the cache file for the blob if it is both allocated and fully loaded in the cache; else, throws
     * an exception
     */
    String getCacheFileFor( final UUID blobId );
    
    
    /**
     * Marks an allocated blob in the cache as being fully loaded
     */
    void blobLoadedToCache( final UUID blobId );
    
    
    /**
     * @return true if the blob is both allocated and fully loaded in the cache
     */
    boolean isInCache( final UUID blobId );
    

    /**
     * @return true if every blob in the specified chunk is (i) allocated, (ii) fully loaded in the cache,
     * and (iii) has a checksum in the database
     */
    boolean isChunkEntirelyInCache( final UUID chunkId );
    
    
    /**
     * @return true if the blob is allocated in the cache, regardless as to whether or not it is fully loaded
     * in the cache
     */
    boolean isCacheSpaceAllocated( final UUID blobId );
    
    
    CacheInformation getCacheState( final boolean includeCacheEntries );


    long getCacheSizeInBytes();

    
    long getMaximumChunkSizeInBytes();
    
    
    void reallocate(
            final UUID blobIdToAllocateFrom,
            final UUID blobIdToAllocateTo, 
            final boolean deleteBlobToAllocateFrom );


    void forceFullCacheReclaimNow();
    

    void registerCacheListener( final CacheListener listener );
    
    
    void unregisterCacheListener( final CacheListener listener );


    void createFilesForZeroLengthChunk( final JobEntry chunk );


    void lockBlobs(final UUID lockHolder, final Set<UUID> blobIds);


    void unlockBlobs(final UUID jobId);

    long getSoonAvailableCapacityInBytes();

    long getImmediatelyAvailableCapacityInBytes();

    long getUsedCapacityInBytes();
}

