/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.WriteOptimization;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.MutableCache;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;

public final class CanAllocatePersistenceTargetSupport
{
    public CanAllocatePersistenceTargetSupport( final BeansServiceManager serviceManager )
    {
        initServiceManager( serviceManager );
    }
    
    
    private static void initServiceManager( final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        s_serviceManager = serviceManager;
    }


    public boolean allowedToTakeAnotherPersistenceTarget(
            final long fullSize,
            final UUID isolatedBucketId,
            final UUID storageDomainId )
    {
        final StorageDomain storageDomain =
                s_serviceManager.getRetriever( StorageDomain.class ).attain( storageDomainId );
        if ( WriteOptimization.PERFORMANCE == storageDomain.getWriteOptimization() )
        {
            return true;
        }

        final BucketStorageDomain key = new BucketStorageDomain( isolatedBucketId, storageDomain.getId() );
        final WriteOptimizationData data = WRITE_OPTIMIZATION_DATA_CACHE.get( key );
        final long totalAvailableBytes = data.getTotalAvailableCapacity( fullSize );
        final BytesRenderer bytesRenderer = new BytesRenderer();
        if ( totalAvailableBytes < data.m_pendingDataToWrite || 0 == totalAvailableBytes )
        {
            if ( shouldLogAllocation( key, data.m_pendingDataToWrite, totalAvailableBytes ) )
            {
                LOG.info( "Media can be allocated to storage domain " + storageDomain.getId()
                        + " for bucket " + isolatedBucketId + ","
                          + " since " + bytesRenderer.render( data.m_pendingDataToWrite )
                          + " of data is waiting to be written and "
                          + bytesRenderer.render( totalAvailableBytes )
                          + " is available on tapes and pools we might be able to use." );
            }
            return true;
        }
        return false;
    }


    private static boolean shouldLogAllocation(
            final BucketStorageDomain key,
            final long pendingDataToWrite,
            final long totalAvailableBytes )
    {
        final long now = System.nanoTime();
        final LogThrottleEntry prev = LOG_THROTTLE.get( key );
        final boolean valuesChanged = prev == null
                || prev.m_pendingDataToWrite != pendingDataToWrite
                || prev.m_totalAvailableBytes != totalAvailableBytes;
        final boolean windowExpired = prev == null
                || now - prev.m_timestampNanos >= LOG_THROTTLE_NANOS;
        if ( valuesChanged || windowExpired )
        {
            LOG_THROTTLE.put( key, new LogThrottleEntry( now, pendingDataToWrite, totalAvailableBytes ) );
            return true;
        }
        return false;
    }


    private final static class LogThrottleEntry
    {
        private LogThrottleEntry(
                final long timestampNanos,
                final long pendingDataToWrite,
                final long totalAvailableBytes )
        {
            m_timestampNanos = timestampNanos;
            m_pendingDataToWrite = pendingDataToWrite;
            m_totalAvailableBytes = totalAvailableBytes;
        }

        private final long m_timestampNanos;
        private final long m_pendingDataToWrite;
        private final long m_totalAvailableBytes;
    } // end inner class def
    
    
    private final static class BucketStorageDomain
    {
        private BucketStorageDomain( final UUID isolatedBucketId, final UUID storageDomainId )
        {
            m_isolatedBucketId = isolatedBucketId; //null if not isolated
            m_storageDomainId = storageDomainId;
        }
        
        
        @Override
        public int hashCode()
        {
            if ( null == m_isolatedBucketId )
            {
                return m_storageDomainId.hashCode();
            }
            return m_isolatedBucketId.hashCode() + m_storageDomainId.hashCode();
        }
        
        
        @Override
        public boolean equals( final Object obj )
        {
            if ( obj == this )
            {
                return true;
            }
            if ( ! ( obj instanceof BucketStorageDomain ) )
            {
                return false;
            }
            
            final BucketStorageDomain other = (BucketStorageDomain)obj;
            final boolean bucketIdsEqual = (other.m_isolatedBucketId == null && m_isolatedBucketId == null)
                    || Objects.equals(other.m_isolatedBucketId, m_isolatedBucketId);
            return ( bucketIdsEqual
                    && other.m_storageDomainId.equals( m_storageDomainId ) );
        }


        private final UUID m_isolatedBucketId;
        private final UUID m_storageDomainId;
    } // end inner class def
    
    
    private final static class WriteOptimizationDataProvider
        implements CacheResultProvider< BucketStorageDomain, WriteOptimizationData>
    {
        public WriteOptimizationData generateCacheResultFor(final BucketStorageDomain bsd )
        {
            // NOTE: potential for future optimization: we do not need distinct cache results for each
            // BucketStorageDomain if bucket is non-isolated, just one per storage domain for that bucket.
            final long pendingSpaceNeeded = s_serviceManager.getService( BucketService.class )
                    .getPendingPutWorkInBytes( bsd.m_isolatedBucketId, bsd.m_storageDomainId );
            final long [] tapeSpaceAvailable = s_serviceManager.getService( TapeService.class )
                    .getAvailableSpacesForBucket( bsd.m_isolatedBucketId, bsd.m_storageDomainId );
            final long [] poolSpaceAvailable = s_serviceManager.getService( PoolService.class )
                    .getAvailableSpacesForBucket( bsd.m_isolatedBucketId, bsd.m_storageDomainId );
            
            final BytesRenderer renderer = new BytesRenderer();
            final String msgTape = ( 0 == tapeSpaceAvailable.length ) ? 
                    "No tapes already allocated."
                    : tapeSpaceAvailable.length + " tapes already allocated.";
            final String msgPool = ( 0 == poolSpaceAvailable.length ) ? 
                    "No pools already allocated."
                    : poolSpaceAvailable.length + " pools already allocated.";
            LOG.info( "For bucket " + bsd.m_isolatedBucketId + " using storage domain " + bsd.m_storageDomainId
                      + ", there is " + renderer.render( pendingSpaceNeeded ) + " pending work.  " 
                      + msgTape + "  " + msgPool );
            
            return new WriteOptimizationData(
                    pendingSpaceNeeded,
                    tapeSpaceAvailable,
                    poolSpaceAvailable );
        }
    } // end inner class def
    
    
    private final static class WriteOptimizationData
    {
        private WriteOptimizationData(
                final long pendingDataToWrite, 
                final long [] tapeSpaceAvailable,
                final long [] poolSpaceAvailable )
        {
            m_pendingDataToWrite = pendingDataToWrite;
            m_tapeSpaceAvailable = tapeSpaceAvailable;
            m_poolSpaceAvailable = poolSpaceAvailable;
        }
        
        private long getTotalAvailableCapacity( final long minBytesToCountMedia )
        {
            long retval = 0;
            for ( final long bytes : m_tapeSpaceAvailable )
            {
                if ( bytes >= minBytesToCountMedia )
                {
                    retval += bytes;
                }
            }
            for ( final long bytes : m_poolSpaceAvailable )
            {
                if ( bytes >= minBytesToCountMedia )
                {
                    retval += bytes;
                }
            }
            
            return retval;
        }
        
        private final long m_pendingDataToWrite;
        private final long [] m_tapeSpaceAvailable;
        private final long [] m_poolSpaceAvailable;
    } // end inner class def
    
    
    public static void clearCachedBucketWriteOptimizationData()
    {
        WRITE_OPTIMIZATION_DATA_CACHE.clearCacheContents();
    }
    
    
    private volatile static BeansServiceManager s_serviceManager;
    private final static MutableCache< BucketStorageDomain, WriteOptimizationData>
            WRITE_OPTIMIZATION_DATA_CACHE =
            new MutableCache<>( 30000, new WriteOptimizationDataProvider() );
    private final static long LOG_THROTTLE_NANOS = TimeUnit.MINUTES.toNanos( 2 );
    private final static ConcurrentHashMap< BucketStorageDomain, LogThrottleEntry > LOG_THROTTLE =
            new ConcurrentHashMap<>();
    private final static Logger LOG = Logger.getLogger( CanAllocatePersistenceTargetSupport.class );
}
