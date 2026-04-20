package com.spectralogic.s3.dataplanner.cache;

import com.spectralogic.s3.common.dao.domain.planner.BlobCache;

public interface AsyncBlobCacheDeleter {
    void delete(BlobCache bc);

    long getBytesPendingDelete();

    long getEntriesPendingDelete();

    void waitUntilCurrentDeletionsDone();
}
