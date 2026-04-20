package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

import java.util.Date;
import java.util.UUID;

@ViewDefinition(
        "SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, s3_blob_destination.blob_store_state as destination_state, s3_blob_destination.target_id, s3_blob_destination.rule_id, s3_blob_destination.id as s3_blob_destination_id" +
                " FROM ds3.job_entry" +
                " LEFT JOIN ds3.s3_blob_destination on entry_id = job_entry.id" +
                " JOIN ds3.job on job_id = job.id" +
                " JOIN ds3.blob on blob_id = blob.id" +
                " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id"
)
@Indexes({
        @Index(S3JobEntryWork.PRIORITY),
        @Index(S3JobEntryWork.CREATED_AT),
        @Index(S3JobEntryWork.JOB_ID),
        @Index(S3JobEntryWork.CHUNK_NUMBER)
})
public interface S3JobEntryWork extends JobEntry, DatabaseView {

    String REQUEST_TYPE = "requestType";

    JobRequestType getRequestType();

    S3JobEntryWork setRequestType(final JobRequestType value );


    String IOM_TYPE = "iomType";

    IomType getIomType();

    S3JobEntryWork setIomType(final IomType value );


    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    UUID getBucketId();

    S3JobEntryWork setBucketId(final UUID value );


    String CREATED_AT = "createdAt";

    @SortBy( value = 2 )
    Date getCreatedAt();

    S3JobEntryWork setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    S3JobEntryWork setPriority(final BlobStoreTaskPriority value );


    String OBJECT_ID = "objectId";

    @References( S3Object.class )
    UUID getObjectId();

    S3JobEntryWork setObjectId(final UUID value );


    String LENGTH = "length";

    long getLength();

    S3JobEntryWork setLength(final long value );


    String CACHE_STATE = "cacheState";

    CacheEntryState getCacheState();

    S3JobEntryWork setCacheState(final CacheEntryState value );


    String TARGET_ID = "targetId";

    @References( S3Target.class )
    UUID getTargetId();

    S3JobEntryWork setTargetId(final UUID value );


    String RULE_ID = "ruleId";

    UUID getRuleId();

    S3JobEntryWork setRuleId(final UUID value );


    String DESTINATION_STATE = "destinationState";

    JobChunkBlobStoreState getDestinationState();

    S3JobEntryWork setDestinationState(final JobChunkBlobStoreState value );


    String S3_BLOB_DESTINATION_ID = "s3BlobDestinationId";

    @References( S3BlobDestination.class )
    UUID getS3BlobDestinationId();

    S3JobEntryWork setS3BlobDestinationId(final UUID value );


    S3JobEntryWork setJobId(final UUID value );
    S3JobEntryWork setChunkNumber(final int value );
}