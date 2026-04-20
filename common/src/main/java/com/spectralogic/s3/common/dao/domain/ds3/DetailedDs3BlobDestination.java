/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

@ViewDefinition(
    "SELECT ds3_blob_destination.*, job_entry.job_id, job.priority, job.created_at" +
    " FROM ds3.ds3_blob_destination" +
    " JOIN ds3.job_entry on entry_id = job_entry.id" +
    " JOIN ds3.job on job_entry.job_id = job.id"
)
@Indexes({
    @Index(DetailedDs3BlobDestination.PRIORITY),
    @Index(DetailedDs3BlobDestination.CREATED_AT),
    @Index(DetailedDs3BlobDestination.JOB_ID)
})
public interface DetailedDs3BlobDestination extends Ds3BlobDestination, DatabaseView
{
    String JOB_ID = "jobId";

    @References(Job.class)
    @SortBy( value = 3)
    UUID getJobId();

    DetailedDs3BlobDestination setJobId(final UUID value);


    String CREATED_AT = "createdAt";

    @SortBy( value = 2)
    Date getCreatedAt();

    DetailedDs3BlobDestination setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    DetailedDs3BlobDestination setPriority(final BlobStoreTaskPriority value);
}