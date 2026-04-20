package com.spectralogic.s3.dataplanner.cache;

import java.util.Set;
import java.util.UUID;

public interface CacheSpaceReclaimer {
    long run(long minFreeBytesBeforeReclaimStopsEarly);

    void lockBlobs(UUID lockHolder, Set<UUID> blobIds);

    void unlockBlobs(UUID lockHolder);
}
