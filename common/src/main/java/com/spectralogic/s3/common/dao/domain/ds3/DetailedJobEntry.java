package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

import java.util.Date;
import java.util.UUID;

@ViewDefinition(
    "SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, blob_cache.size_in_bytes as cache_size_in_bytes" +
    " FROM ds3.job_entry" +
    " JOIN ds3.job on job_id = job.id" +
    " JOIN ds3.blob on blob_id = blob.id" +
    " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id"
)
@Indexes({
    @Index(DetailedJobEntry.PRIORITY),
    @Index(DetailedJobEntry.CREATED_AT),
    @Index(DetailedJobEntry.JOB_ID),
    @Index(DetailedJobEntry.CHUNK_NUMBER)
})
public interface DetailedJobEntry extends JobEntry, DatabaseView {

    String REQUEST_TYPE = "requestType";

    JobRequestType getRequestType();

    DetailedJobEntry setRequestType( final JobRequestType value );


    String IOM_TYPE = "iomType";

    IomType getIomType();

    DetailedJobEntry setIomType(final IomType value );


    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    UUID getBucketId();

    DetailedJobEntry setBucketId( final UUID value );


    String CREATED_AT = "createdAt";

    @SortBy( value = 2 )
    Date getCreatedAt();

    DetailedJobEntry setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    DetailedJobEntry setPriority(final BlobStoreTaskPriority value );


    String OBJECT_ID = "objectId";

    @References( S3Object.class )
    UUID getObjectId();

    DetailedJobEntry setObjectId( final UUID value );


    String LENGTH = "length";

    long getLength();

    DetailedJobEntry setLength( final long value );


    String CACHE_STATE = "cacheState";

    CacheEntryState getCacheState();

    DetailedJobEntry setCacheState(final CacheEntryState value );


    String CACHE_SIZE_IN_BYTES = "cacheSizeInBytes";

    @Optional
    Long getCacheSizeInBytes();

    DetailedJobEntry setCacheSizeInBytes( final Long value );


    DetailedJobEntry setJobId(final UUID value );
    DetailedJobEntry setChunkNumber(final int value );
}
