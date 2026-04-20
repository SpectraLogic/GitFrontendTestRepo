package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

import java.util.Date;
import java.util.UUID;

@ViewDefinition(
        "SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, local_blob_destination.blob_store_state as destination_state, local_blob_destination.storage_domain_id, local_blob_destination.isolated_bucket_id, local_blob_destination.persistence_rule_id, blob_tape.order_index, local_blob_destination.id as local_blob_destination_id" +
                " FROM ds3.job_entry" +
                " LEFT JOIN ds3.local_blob_destination on entry_id = job_entry.id" +
                " JOIN ds3.job on job_id = job.id" +
                " JOIN ds3.blob on blob_id = blob.id" +
                " LEFT JOIN tape.blob_tape on blob.id = blob_tape.blob_id AND blob_tape.tape_id = read_from_tape_id" +
                " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id"
)

@Indexes({
        @Index(LocalJobEntryWork.ORDER_INDEX), //NOTE: this value will only be non-null on reads/verifies from tape
        @Index(LocalJobEntryWork.PRIORITY),
        @Index(LocalJobEntryWork.CREATED_AT),
        @Index(LocalJobEntryWork.JOB_ID),
        @Index(LocalJobEntryWork.CHUNK_NUMBER)
})
public interface LocalJobEntryWork extends JobEntry, OrderedEntry<JobEntry>, DatabaseView {

    String REQUEST_TYPE = "requestType";

    JobRequestType getRequestType();

    LocalJobEntryWork setRequestType(final JobRequestType value );


    String IOM_TYPE = "iomType";

    IomType getIomType();

    LocalJobEntryWork setIomType(final IomType value );


    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    UUID getBucketId();

    LocalJobEntryWork setBucketId(final UUID value );


    String CREATED_AT = "createdAt";

    @SortBy( value = 2 )
    Date getCreatedAt();

    LocalJobEntryWork setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    LocalJobEntryWork setPriority(final BlobStoreTaskPriority value );


    String OBJECT_ID = "objectId";

    @References( S3Object.class )
    UUID getObjectId();

    LocalJobEntryWork setObjectId(final UUID value );


    String LENGTH = "length";

    long getLength();

    LocalJobEntryWork setLength(final long value );


    String CACHE_STATE = "cacheState";

    CacheEntryState getCacheState();

    LocalJobEntryWork setCacheState(final CacheEntryState value );


    String STORAGE_DOMAIN_ID = "storageDomainId";

    @References( StorageDomain.class )
    UUID getStorageDomainId();

    LocalJobEntryWork setStorageDomainId(final UUID value );


    String ISOLATED_BUCKET_ID = "isolatedBucketId";

    @References( Bucket.class )
    UUID getIsolatedBucketId();

    LocalJobEntryWork setIsolatedBucketId(final UUID value );


    String PERSISTENCE_RULE_ID = "persistenceRuleId";

    @References( DataPersistenceRule.class )
    UUID getPersistenceRuleId();

    LocalJobEntryWork setPersistenceRuleId(final UUID value );


    String DESTINATION_STATE = "destinationState";

    JobChunkBlobStoreState getDestinationState();

    LocalJobEntryWork setDestinationState(final JobChunkBlobStoreState value );


    String LOCAL_BLOB_DESTINATION_ID = "localBlobDestinationId";

    @References( LocalBlobDestination.class )
    UUID getLocalBlobDestinationId();

    LocalJobEntryWork setLocalBlobDestinationId(final UUID value );


    LocalJobEntryWork setBlobId( final UUID value );
    LocalJobEntryWork setJobId(final UUID value );
    LocalJobEntryWork setChunkNumber(final int value );
}
