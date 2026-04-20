/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

@ViewDefinition(
    "SELECT local_blob_destination.*, job.request_type, job.iom_type, job.bucket_id, job_entry.job_id, job_entry.chunk_number, job.priority, job.created_at" +
    " FROM ds3.local_blob_destination" +
    " JOIN ds3.job_entry on entry_id = job_entry.id" +
    " JOIN ds3.job on job_entry.job_id = job.id"
)
@Indexes({
    @Index(DetailedLocalBlobDestination.PRIORITY),
    @Index(DetailedLocalBlobDestination.CREATED_AT),
    @Index(DetailedLocalBlobDestination.JOB_ID),
    @Index(DetailedLocalBlobDestination.CHUNK_NUMBER)
})
public interface DetailedLocalBlobDestination extends LocalBlobDestination, DatabaseView
{
    String REQUEST_TYPE = "requestType";

    JobRequestType getRequestType();

    DetailedLocalBlobDestination setRequestType( final JobRequestType value );


    String IOM_TYPE = "iomType";

    IomType getIomType();

    DetailedLocalBlobDestination setIomType(final IomType value );


    String BUCKET_ID = "bucketId";

    @References( Bucket.class )
    UUID getBucketId();

    DetailedLocalBlobDestination setBucketId( final UUID value );

    String JOB_ID = "jobId";

    @References(Job.class)
    @SortBy( value = 3 )
    UUID getJobId();

    DetailedLocalBlobDestination setJobId(final UUID value );


    String CREATED_AT = "createdAt";

    @SortBy( value = 2 )
    Date getCreatedAt();

    DetailedLocalBlobDestination setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    DetailedLocalBlobDestination setPriority(final BlobStoreTaskPriority value );


    String CHUNK_NUMBER = "chunkNumber";

    @SortBy( value = 4 )
    Integer getChunkNumber();

    DetailedLocalBlobDestination setChunkNumber(final Integer value);
}
