package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

import java.util.Date;
import java.util.UUID;

@ViewDefinition(
        "SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, ds3_blob_destination.blob_store_state as destination_state, ds3_blob_destination.target_id, ds3_blob_destination.rule_id, ds3_blob_destination.id as ds3_blob_destination_id" +
                " FROM ds3.job_entry" +
                " LEFT JOIN ds3.ds3_blob_destination on entry_id = job_entry.id" +
                " JOIN ds3.job on job_id = job.id" +
                " JOIN ds3.blob on blob_id = blob.id" +
                " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id"
)
@Indexes({
        @Index(Ds3JobEntryWork.PRIORITY),
        @Index(Ds3JobEntryWork.CREATED_AT),
        @Index(Ds3JobEntryWork.JOB_ID),
        @Index(Ds3JobEntryWork.CHUNK_NUMBER)
})
public interface Ds3JobEntryWork extends JobEntry, DatabaseView {

    String REQUEST_TYPE = "requestType";

    JobRequestType getRequestType();

    Ds3JobEntryWork setRequestType(final JobRequestType value );


    String IOM_TYPE = "iomType";

    IomType getIomType();

    Ds3JobEntryWork setIomType(final IomType value );


    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    UUID getBucketId();

    Ds3JobEntryWork setBucketId(final UUID value );


    String CREATED_AT = "createdAt";

    @SortBy( value = 2 )
    Date getCreatedAt();

    Ds3JobEntryWork setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    Ds3JobEntryWork setPriority(final BlobStoreTaskPriority value );


    String OBJECT_ID = "objectId";

    @References( S3Object.class )
    UUID getObjectId();

    Ds3JobEntryWork setObjectId(final UUID value );


    String LENGTH = "length";

    long getLength();

    Ds3JobEntryWork setLength(final long value );


    String CACHE_STATE = "cacheState";

    CacheEntryState getCacheState();

    Ds3JobEntryWork setCacheState(final CacheEntryState value );


    String TARGET_ID = "targetId";

    @References( Ds3Target.class )
    UUID getTargetId();

    Ds3JobEntryWork setTargetId(final UUID value );


    String RULE_ID = "ruleId";

    UUID getRuleId();

    Ds3JobEntryWork setRuleId(final UUID value );


    String DESTINATION_STATE = "destinationState";

    JobChunkBlobStoreState getDestinationState();

    Ds3JobEntryWork setDestinationState(final JobChunkBlobStoreState value );


    String DS3_BLOB_DESTINATION_ID = "ds3BlobDestinationId";

    @References( Ds3BlobDestination.class )
    UUID getDs3BlobDestinationId();

    Ds3JobEntryWork setDs3BlobDestinationId(final UUID value );


    Ds3JobEntryWork setJobId(final UUID value );
    Ds3JobEntryWork setChunkNumber(final int value );
}