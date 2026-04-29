package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.render.BytesRenderer;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.spectralogic.util.tunables.Tunables;

final class CacheSpaceReclaimerImpl implements CacheSpaceReclaimer {

    public CacheSpaceReclaimerImpl(BlobCacheService blobCacheService, final AsyncBlobCacheDeleter asyncBlobCacheDeleter, final long filesystemOverheadPerBlob ) {
        m_asyncBlobCacheDeleter = asyncBlobCacheDeleter;
        m_blobCacheService = blobCacheService;
        m_filesystemOverheadPerBlob = filesystemOverheadPerBlob;
    }

    @Override
    synchronized public long run( final long minReclaimedBytesBeforeReclaimStopsEarly) {
        long retval = 0;
        synchronized (m_reclaimLock) {
            for (final UUID retiredLockholder : m_retiredLockholders) {
                m_lockedBlobs.remove(retiredLockholder);
            }
        }
        if (m_blobCacheService.deletedBlobCachesExist()) {
            try (final EnhancedIterable<BlobCache> deletedBlobCaches = m_blobCacheService.getDeletedBlobCaches()) {
                retval += reclaim(deletedBlobCaches, "deleted blobs", minReclaimedBytesBeforeReclaimStopsEarly, m_blobCacheService);
            }
        }

        try (final EnhancedIterable<BlobCache> lruSortedblobEntries = getSortedBlobCachesForReclaim(m_blobCacheService)) {
            retval += reclaim(lruSortedblobEntries, "managed blobs", minReclaimedBytesBeforeReclaimStopsEarly, m_blobCacheService);
        }
        return retval;
    }

    private long reclaim(final EnhancedIterable<BlobCache> lruSortedblobEntries,
                         final String blobsDescription,
                         final long minReclaimedBytesBeforeReclaimStopsEarly,
                         final BlobCacheService bcs) {
        final AtomicLong releasedSize = new AtomicLong();
        final AtomicInteger releasedCount = new AtomicInteger();
        final Duration duration = new Duration();
        final Set<BlobCache> workingSet = new HashSet<>();
        long workingBytes = 0;

        for (BlobCache bc : lruSortedblobEntries) {
            workingSet.add(bc);
            workingBytes += bc.getSizeInBytes();
            if (Tunables.cacheSpaceReclaimerMaxReclaimSegmentBlobs() == workingSet.size() || Tunables.cacheSpaceReclaimerMaxReclaimSegmentBytes() <= workingBytes) {
                release(workingSet, releasedSize, releasedCount, bcs);
                workingSet.clear();
                workingBytes = 0;

                if (releasedSize.get() >= minReclaimedBytesBeforeReclaimStopsEarly) {
                    LOG.info("Cache reclaim cycle reached min release threshold (stopped early).");
                    break;
                }
            }
        }
        release(workingSet, releasedSize, releasedCount, bcs);
        final BytesRenderer bytesRenderer = new BytesRenderer();
        LOG.info("Cache reclaim cycle for " + blobsDescription + " completed in " + duration
                + ".  Reclaimed " + releasedCount.get() + " blobs to free up "
                + bytesRenderer.render(releasedSize.get()) + ".");
        return releasedSize.get();
    }


    private void release(
            final Set<BlobCache> blobEntries,
            final AtomicLong releasedSize,
            final AtomicInteger releasedCount,
            final BlobCacheService bcs) {
        final long initialSize = blobEntries.size();
        for (final Map.Entry<UUID, Set<UUID>> entry : m_lockedBlobs.entrySet()) {
            if (blobEntries.removeIf(blobCache -> entry.getValue().contains(blobCache.getBlobId()))) {
                LOG.info("Cache reclaim cycle skipped " + (initialSize - blobEntries.size())
                        + " blobs locked by job " + entry.getKey() + ".");
            }
        }
        final Set<UUID> blobCacheIds = BeanUtils.extractPropertyValues(blobEntries, Identifiable.ID);
        bcs.update( Require.beanPropertyEqualsOneOf(Identifiable.ID, blobCacheIds),
                (bc) -> bc.setState( CacheEntryState.PENDING_DELETE ),
                BlobCache.STATE );
        for (final BlobCache bc : blobEntries) {
            releasedSize.addAndGet(bc.getSizeInBytes() + m_filesystemOverheadPerBlob);
            releasedCount.incrementAndGet();
            m_asyncBlobCacheDeleter.delete(bc);
        }
    }


    //Sorted list of blob cache entries for managed (not deleted) blobs that do not have associated job entries.
    private EnhancedIterable<BlobCache> getSortedBlobCachesForReclaim(final BlobCacheService bcs) {
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add(BlobCache.LAST_ACCESSED, SortBy.Direction.ASCENDING);
        final Query.LimitableRetrievable query = Query.where(
                Require.all(
                        Require.beanPropertyNotNull(BlobCache.BLOB_ID),
                        Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)),
                        Require.not(
                                Require.exists(
                                        BlobCache.BLOB_ID,
                                        Require.exists(
                                                JobEntry.class,
                                                JobEntry.BLOB_ID,
                                                Require.nothing()
                                        )
                                )
                        )
                )
        ).orderBy(ordering);
        return bcs.retrieveAll(query).toIterable();
    }

    @Override
    public void lockBlobs(final UUID lockHolder, final Set<UUID> blobIds) {
        synchronized (m_reclaimLock) {
            m_retiredLockholders.remove(lockHolder); //In general lockholders will not be reused, but we do this just in case.
            m_lockedBlobs.put(lockHolder, blobIds);
        }
    }

    @Override
    public void unlockBlobs(final UUID lockHolder) {
        synchronized (m_reclaimLock) {
            //NOTE: The blobs locked by this lock holder will have gained job entries before this method is called.
            //However, they may have already been selected by the LRU query, so we will keep these blobs locked until the
            //next LRU query instead of releasing them immediately.
            m_retiredLockholders.add(lockHolder);
        }
    }

    private final Map<UUID, Set<UUID>> m_lockedBlobs = new HashMap<>();
    private final Set<UUID> m_retiredLockholders = new HashSet<>();
    private final BlobCacheService m_blobCacheService;
    private final long m_filesystemOverheadPerBlob;
    private final AsyncBlobCacheDeleter m_asyncBlobCacheDeleter;
    private final Object m_reclaimLock = new Object();
    private final static Logger LOG = Logger.getLogger( CacheManagerImpl.class );
}
