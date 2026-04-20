package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

//NOTE: this is more of a wrapper for test-use than a mock since it wraps a real instance of cache space reclaimer
public class MockCacheSpaceReclaimer implements CacheSpaceReclaimer {

    public MockCacheSpaceReclaimer(final DatabaseSupport dbSupport, final AsyncBlobCacheDeleter asyncBlobCacheDeleter, long filesystemOverheadPerBlob) {
        m_cacheSpaceReclaimer = new CacheSpaceReclaimerImpl(dbSupport.getServiceManager().getService(BlobCacheService.class), asyncBlobCacheDeleter, filesystemOverheadPerBlob);
    }

    @Override
    public long run(long minFreeBytesBeforeReclaimStopsEarly) {
        final long retval = m_cacheSpaceReclaimer.run(minFreeBytesBeforeReclaimStopsEarly);
        m_runs.add(Pair.of(minFreeBytesBeforeReclaimStopsEarly, retval));
        return retval;
    }

    @Override
    public void lockBlobs(UUID lockHolder, Set<UUID> blobIds) {
        m_cacheSpaceReclaimer.lockBlobs(lockHolder, blobIds);
    }

    @Override
    public void unlockBlobs(UUID lockHolder) {
        m_cacheSpaceReclaimer.unlockBlobs(lockHolder);
    }

    public boolean neverAttemptedToReclaimMoreThan(final long bytes) {
        for (final Pair<Long, Long> run : m_runs) {
            if (run.getLeft() > bytes) {
                return false;
            }
        }
        return true;
    }

    public boolean attemptedToReclaim(final long bytes) {
        for (final Pair<Long, Long> run : m_runs) {
            if (run.getLeft() == bytes) {
                return true;
            }
        }
        return false;
    }

    public boolean reclaimAttempted() {
        for (final Pair<Long, Long> run : m_runs) {
            if (run.getLeft() > 0) {
                return true;
            }
        }
        return false;
    }

    final CacheSpaceReclaimer m_cacheSpaceReclaimer;
    final ArrayList<Pair<Long, Long>> m_runs = new ArrayList<>();
}
