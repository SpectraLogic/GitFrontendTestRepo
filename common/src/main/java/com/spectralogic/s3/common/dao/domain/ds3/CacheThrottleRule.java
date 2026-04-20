package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.*;

import java.util.UUID;

@UniqueIndexes(
        {
                @Unique({CacheThrottleRule.PRIORITY, CacheThrottleRule.BUCKET_ID, CacheThrottleRule.REQUEST_TYPE})
        })
public interface CacheThrottleRule extends DatabasePersistable {
    String PRIORITY = "priority";

    @Optional
    BlobStoreTaskPriority getPriority();

    CacheThrottleRule setPriority(final BlobStoreTaskPriority value);


    String BUCKET_ID = "bucketId";

    @References(Bucket.class)
    @CascadeDelete(CascadeDelete.WhenReferenceIsDeleted.DELETE_THIS_BEAN)
    @Optional
    UUID getBucketId();

    CacheThrottleRule setBucketId(final UUID value);


    String REQUEST_TYPE = "requestType";

    @Optional
    JobRequestType getRequestType();

    CacheThrottleRule setRequestType(final JobRequestType value);


    String MAX_CACHE_PERCENT = "maxCachePercent";

    double getMaxCachePercent();

    CacheThrottleRule setMaxCachePercent(final double value);


    String BURST_THRESHOLD = "burstThreshold";

    @Optional
    Double getBurstThreshold();

    CacheThrottleRule setBurstThreshold(final Double value);
}
