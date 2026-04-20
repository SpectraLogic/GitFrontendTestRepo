package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

import java.util.Date;
import java.util.UUID;

@ViewDefinition(
        "SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, azure_blob_destination.blob_store_state as destination_state, azure_blob_destination.target_id, azure_blob_destination.rule_id, azure_blob_destination.id as azure_blob_destination_id" +
                " FROM ds3.job_entry" +
                " LEFT JOIN ds3.azure_blob_destination on entry_id = job_entry.id" +
                " JOIN ds3.job on job_id = job.id" +
                " JOIN ds3.blob on blob_id = blob.id" +
                " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id"
)
@Indexes({
        @Index(AzureJobEntryWork.PRIORITY),
        @Index(AzureJobEntryWork.CREATED_AT),
        @Index(AzureJobEntryWork.JOB_ID),
        @Index(AzureJobEntryWork.CHUNK_NUMBER)
})
public interface AzureJobEntryWork extends JobEntry, DatabaseView {

    String REQUEST_TYPE = "requestType";

    JobRequestType getRequestType();

    AzureJobEntryWork setRequestType(final JobRequestType value );


    String IOM_TYPE = "iomType";

    IomType getIomType();

    AzureJobEntryWork setIomType(final IomType value );


    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    UUID getBucketId();

    AzureJobEntryWork setBucketId(final UUID value );


    String CREATED_AT = "createdAt";

    @SortBy( value = 2 )
    Date getCreatedAt();

    AzureJobEntryWork setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    AzureJobEntryWork setPriority(final BlobStoreTaskPriority value );


    String OBJECT_ID = "objectId";

    @References( S3Object.class )
    UUID getObjectId();

    AzureJobEntryWork setObjectId(final UUID value );


    String LENGTH = "length";

    long getLength();

    AzureJobEntryWork setLength(final long value );


    String CACHE_STATE = "cacheState";

    CacheEntryState getCacheState();

    AzureJobEntryWork setCacheState(final CacheEntryState value );


    String TARGET_ID = "targetId";

    @References( AzureTarget.class )
    UUID getTargetId();

    AzureJobEntryWork setTargetId(final UUID value );


    String RULE_ID = "ruleId";

    UUID getRuleId();

    AzureJobEntryWork setRuleId(final UUID value );


    String DESTINATION_STATE = "destinationState";

    JobChunkBlobStoreState getDestinationState();

    AzureJobEntryWork setDestinationState(final JobChunkBlobStoreState value );


    String AZURE_BLOB_DESTINATION_ID = "azureBlobDestinationId";

    @References( AzureBlobDestination.class )
    UUID getAzureBlobDestinationId();

    AzureJobEntryWork setAzureBlobDestinationId(final UUID value );


    AzureJobEntryWork setJobId(final UUID value );
    AzureJobEntryWork setChunkNumber(final int value );
}