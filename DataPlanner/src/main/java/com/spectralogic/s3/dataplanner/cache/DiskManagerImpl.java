/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.cache;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.cache.CacheListener;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheInformation;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class DiskManagerImpl implements DiskManager
{

    public DiskManagerImpl( final CacheManager cacheManager )
    {
        this( cacheManager, null );
    }

    public DiskManagerImpl( final CacheManager cacheManager, final PoolBlobStore poolBlobStore )
    {
        m_cacheManager = cacheManager;
        m_poolBlobStore = poolBlobStore;
        Validations.verifyNotNull( "Cache manager", m_cacheManager );
    }


    public DiskFileInfo getDiskFileFor(final UUID blobId )
    {
        String filePath = m_cacheManager.getCacheFileFor( blobId );
        if (filePath != null) { //file in cache
            return BeanFactory.newBean(DiskFileInfo.class)
                    .setFilePath(filePath);
        } else { //file on pool
            DiskFileInfo diskFileInfo = null;
            if ( null != m_poolBlobStore )
            {
                diskFileInfo = m_poolBlobStore.getPoolFileFor( blobId );
            }
            if ( null != diskFileInfo)
            {
                return diskFileInfo;

            }
        }
        throw new DataPlannerException(
                GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT,
                "Blob not in cache yet: " + blobId );
    }
    
    
    public boolean isOnDisk( final UUID blobId )
    {
        return m_cacheManager.isInCache( blobId ) ||
                ( null != m_poolBlobStore && m_poolBlobStore.isAvailableOnPool( blobId ) );
    }
    
    
    public boolean isChunkEntirelyOnDisk( final UUID chunkId )
    {
        return m_cacheManager.isChunkEntirelyInCache( chunkId ) || 
                ( null != m_poolBlobStore && m_poolBlobStore.isChunkEntirelyAvailableOnPool( chunkId ) );
    }


    public void allocateChunks(final Set< UUID > chunkIds )
    {
        m_cacheManager.allocateChunks( chunkIds );
    }


    public void allocateChunk(final UUID chunkId )
    {
        m_cacheManager.allocateChunk( chunkId );
    }


    public String allocateChunksForBlob(final UUID blobId )
    {
        return m_cacheManager.allocateChunksForBlob( blobId );
    }
    
    
    public String getCacheFileFor( final UUID blobId )
    {
        return m_cacheManager.getCacheFileFor( blobId );
    }
    
    
    public void blobLoadedToCache( final UUID blobId )
    {
        m_cacheManager.blobLoadedToCache( blobId );
    }
    
    
    public boolean isInCache( final UUID blobId )
    {
        return m_cacheManager.isInCache( blobId );
    }
    
    
    public boolean isChunkEntirelyInCache( final UUID chunkId )
    {
        return m_cacheManager.isChunkEntirelyInCache( chunkId );
    }
    
    
    public boolean isCacheSpaceAllocated( final UUID blobId )
    {
        return m_cacheManager.isCacheSpaceAllocated( blobId );
    }
    
    
    public CacheInformation getCacheState( final boolean includeCacheEntries )
    {
        return m_cacheManager.getCacheState( includeCacheEntries );
    }


    public long getCacheSizeInBytes()
    {
        return m_cacheManager.getCacheSizeInBytes();
    }
    
    
    public long getMaximumChunkSizeInBytes()
    {
        return m_cacheManager.getMaximumChunkSizeInBytes();
    }
    
    
    public void reallocate(
            final UUID blobIdToAllocateFrom,
            final UUID blobIdToAllocateTo, 
            final boolean deleteBlobToAllocateFrom )
    {
        m_cacheManager.reallocate( blobIdToAllocateFrom, blobIdToAllocateTo, deleteBlobToAllocateFrom );
    }


    public void forceFullCacheReclaimNow()
    {
        m_cacheManager.forceFullCacheReclaimNow();
    }
    
    
    public void processDeleteResults( final S3ObjectService.DeleteResult deleteResult )
    {
        final Set< Future< ? > > futures = new HashSet<>();
        if ( null != m_poolBlobStore )
        {
            futures.add( m_objectDeleterWorkPool.submit(
                    () -> m_poolBlobStore.deleteObjects( deleteResult.getBucketName(),
                            deleteResult.getObjectIds() ) ) );
        }
        futures.forEach( x -> {
            try
            {
                x.get();
            }
            catch ( final ExecutionException | InterruptedException e )
            {
                LOG.warn( "Failed waiting for future from deleting objects and blobs", e );
            }
        } );
    }
    
    
    public void deleteBucket( final UUID bucketId, final String bucketName )
    {
        if ( null != m_poolBlobStore )
        {
            m_poolBlobStore.deleteBucket( bucketId, bucketName );
        }
    }
    
    public void createFilesForZeroLengthChunk( final JobEntry chunk )
    {
    	m_cacheManager.createFilesForZeroLengthChunk( chunk );
    }
    
    
    public void registerCacheListener( final CacheListener listener )
    {
    	m_cacheManager.registerCacheListener( listener );
    }
    
    
    public void unregisterCacheListener( final CacheListener listener )
    {
    	m_cacheManager.unregisterCacheListener( listener );
    }


    public void lockBlobs(final UUID lockHolder, final Set<UUID> blobIds) {
        m_cacheManager.lockBlobs(lockHolder, blobIds);
    }


    public void unlockBlobs(final UUID lockHolder) {
        m_cacheManager.unlockBlobs(lockHolder);
    }


    @Override
    public long getSoonAvailableCapacityInBytes() {
        return m_cacheManager.getSoonAvailableCapacityInBytes();
    }


    @Override
    public long getImmediatelyAvailableCapacityInBytes() {
        return m_cacheManager.getImmediatelyAvailableCapacityInBytes();
    }

    @Override
    public long getUsedCapacityInBytes() {
        return m_cacheManager.getUsedCapacityInBytes();
    }


    private final CacheManager m_cacheManager;
    private final PoolBlobStore m_poolBlobStore;
    private final WorkPool m_blobDeleterWorkPool = WorkPoolFactory.createWorkPool( 1, "BlobCacheFileDeleter" );
    private final WorkPool m_objectDeleterWorkPool = WorkPoolFactory.createWorkPool( 1, "ObjectPoolFileDeleter" );
    private final static Logger LOG = Logger.getLogger( DiskManagerImpl.class );
}
