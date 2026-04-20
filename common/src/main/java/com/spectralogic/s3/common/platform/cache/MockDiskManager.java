/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.platform.cache;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheInformation;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Platform;

public final class MockDiskManager implements DiskManager
{
    public DiskFileInfo getDiskFileFor(UUID blobId)
    {
        return BeanFactory.newBean(DiskFileInfo.class)
                .setFilePath(getCacheFileFor(blobId));
    }


    public boolean isOnDisk(UUID blobId)
    {
        BlobPool val = m_serviceManager.getRetriever( BlobPool.class )
                .retrieveAll( Require.all( Require.beanPropertyEquals( BlobObservable.BLOB_ID, blobId ))).getFirst();

        return val != null || isInCache( blobId );
    }


    public boolean isChunkEntirelyOnDisk(UUID chunkId)
    {
        return isChunkEntirelyInCache( chunkId );
    }
    
    
    @Override public void processDeleteResults( final S3ObjectService.DeleteResult deleteResult )
    {
    
    }
    
    
    @Override public void deleteBucket( final UUID bucketId, final String bucketName )
    {
    
    }
    
    
    public MockDiskManager( final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
    }
    
    
    public void setOutOfSpace( final boolean value )
    {
        m_outOfSpace = value;
    }
    
    
    public void allocateChunk(final UUID chunkId )
    {
        if ( m_outOfSpace )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT, "Out of space." );
        }
    }
    
    
    public void allocateChunks(final Set< UUID > chunkId )
    {
        if ( m_outOfSpace )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT, "Out of space." );
        }
    }

    
    public String allocateChunksForBlob(final UUID blobId )
    {
        if ( m_outOfSpace )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT, "Out of space." );
        }
        
        m_cache.put( blobId, Boolean.FALSE );
        return getCacheFileFor( blobId );
    }

    
    public void blobLoadedToCache( final UUID blobId )
    {
        m_cache.put( blobId, Boolean.TRUE );
    }

    
    public void deleteCacheFor( final UUID blobId )
    {
        m_cache.remove( blobId );
    }

    
    public String getCacheFileFor( final UUID blobId )
    {
        return System.getProperty( "java.tmpdir" ) + Platform.FILE_SEPARATOR + getClass().getSimpleName()
                + Platform.FILE_SEPARATOR + blobId.toString();
    }

    
    public boolean isInCache( final UUID blobId )
    {
        return ( Boolean.TRUE.equals( m_cache.get( blobId ) ) );
    }
    
    
    public boolean isChunkEntirelyInCache( final UUID chunkId )
    {
        if ( null == m_serviceManager )
        {
            return false;
        }
        return isInCache(m_serviceManager.getRetriever(JobEntry.class).retrieve(chunkId).getBlobId());
    }
    
    
    public boolean isCacheSpaceAllocated( final UUID blobId )
    {
        return ( null != m_cache.get( blobId ) );
    }

    
    public void unusedCacheSpaceChanged()
    {
        throw new UnsupportedOperationException( "No code written for method." );
    }
    

    public void createFilesForZeroLengthChunk( final JobEntry chunk )
    {
        m_cache.put( chunk.getBlobId(), Boolean.TRUE );
    }

    @Override
    public void lockBlobs(UUID lockHolder, Set<UUID> blobIds) {
        // empty
    }

    @Override
    public void unlockBlobs(UUID jobId) {
        // empty
    }

    @Override
    public long getSoonAvailableCapacityInBytes() {
        throw new UnsupportedOperationException( "No code written for method." );
    }

    @Override
    public long getImmediatelyAvailableCapacityInBytes() {
        throw new UnsupportedOperationException( "No code written for method." );
    }

    @Override
    public long getUsedCapacityInBytes() {
        throw new UnsupportedOperationException( "No code written for method." );
    }


    public CacheInformation getCacheState( final boolean includeCacheEntries )
    {
        throw new UnsupportedOperationException( "No code written for method." );
    }


    public long getCacheSizeInBytes()
    {
        return -1;
    }


    public long getMaximumChunkSizeInBytes()
    {
        return Long.MAX_VALUE;
    }
    
    
    public void reallocate(
            UUID blobIdToAllocateFrom,
            UUID blobIdToAllocateTo,
            boolean deleteBlobToAllocateFrom )
    {
        // empty
    }


    public void forceFullCacheReclaimNow()
    {
        
        m_cache.clear();
    }
    
    
    public void deleteBlobs( final Set< UUID > blobIds )
    {
		m_cache.keySet().removeAll( blobIds );
    }
    
    
    public void registerCacheListener( final CacheListener listener )
    {
    	//empty
    }
    
    
    public void unregisterCacheListener( final CacheListener listener )
    {
    	//empty
    }
    
    
    private volatile boolean m_outOfSpace;
    private final BeansServiceManager m_serviceManager;
    private final Map< UUID, Boolean > m_cache = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock m_reclaimLock = new ReentrantReadWriteLock( true );
}
