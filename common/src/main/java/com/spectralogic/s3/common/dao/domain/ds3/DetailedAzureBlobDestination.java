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
    "SELECT azure_blob_destination.*, job_entry.job_id, job.priority, job.created_at" +
    " FROM ds3.azure_blob_destination" +
    " JOIN ds3.job_entry on entry_id = job_entry.id" +
    " JOIN ds3.job on job_entry.job_id = job.id"
)
@Indexes({
    @Index(DetailedAzureBlobDestination.PRIORITY),
    @Index(DetailedAzureBlobDestination.CREATED_AT),
    @Index(DetailedAzureBlobDestination.JOB_ID)
})
public interface DetailedAzureBlobDestination extends AzureBlobDestination, DatabaseView
{
    String JOB_ID = "jobId";

    @References(Job.class)
    @SortBy( value = 3)
    UUID getJobId();

    DetailedAzureBlobDestination setJobId(final UUID value);


    String CREATED_AT = "createdAt";

    @SortBy( value = 2)
    Date getCreatedAt();

    DetailedAzureBlobDestination setCreatedAt(final Date value);


    String PRIORITY = "priority";

    @SortBy( value = 1 )
    BlobStoreTaskPriority getPriority();

    DetailedAzureBlobDestination setPriority(final BlobStoreTaskPriority value);
}