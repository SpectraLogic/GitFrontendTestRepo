package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.service.planner.BlobCacheService;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class MockAsyncBlobCacheDeleter implements AsyncBlobCacheDeleter {

    public MockAsyncBlobCacheDeleter(final DatabaseSupport dbsupport) {
        m_blobCacheService = dbsupport.getServiceManager().getService(BlobCacheService.class);
    }

    @Override
    public void delete(BlobCache bc) {
        m_unavailableBytes += bc.getSizeInBytes();
        m_unavailableEntries++;
        m_blobCacheService.update(bc.setState(CacheEntryState.PENDING_DELETE), BlobCache.STATE);
    }

    @Override
    public long getBytesPendingDelete() {
        return m_unavailableBytes;
    }

    @Override
    public long getEntriesPendingDelete() {
        return m_unavailableEntries;
    }

    @Override
    public void waitUntilCurrentDeletionsDone() {
        m_waitUntilDeletionsDoneCallCount.incrementAndGet();
        for (final BlobCache bc : m_blobCacheService.retrieveAll(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)).toSet()) {
            try {
                if (Files.exists(Paths.get(bc.getPath()))) {
                    Files.delete(Paths.get(bc.getPath()));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        m_blobCacheService.delete(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE));
        m_unavailableBytes = 0;
        m_unavailableEntries = 0;
    }

    public int getWaitUntilDeletionsDoneCallCount()
    {
        return m_waitUntilDeletionsDoneCallCount.get();
    }

    private long m_unavailableBytes = 0;
    private long m_unavailableEntries = 0;
    private final AtomicInteger m_waitUntilDeletionsDoneCallCount = new AtomicInteger( 0 );
    final private BlobCacheService m_blobCacheService;
}
