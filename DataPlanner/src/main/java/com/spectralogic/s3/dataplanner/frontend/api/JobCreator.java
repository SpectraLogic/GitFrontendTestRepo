/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.api;

import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BaseCreateJobParams;
import com.spectralogic.s3.dataplanner.cache.JobCreatedListener;
import com.spectralogic.util.bean.lang.Identifiable;

public interface JobCreator
{
    /**
     * @return the {@link Identifiable#ID} of the {@link Job} created
     */


    UUID createGetOrVerifyJob(
            BaseCreateJobParams<?> params,
            UUID jobId,
            JobRequestType requestType,
            JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee,
            List<JobEntry> jobEntries);

    UUID createPutJob(
            BaseCreateJobParams<?> params,
            UUID jobId,
            S3ObjectCreator objectCreator,
            List<JobEntry> jobEntries);

    void addJobCreatedListener(final JobCreatedListener listener );
    
    
    void notifyJobCreatedListeners( final JobRequestType type, final UUID jobId );
    
    
    /**
     * @return the preferred blob size that will be used when creating jobs
     */
    long getPreferredBlobSizeInBytes();
    
    
    Object getJobReshapingLock();
}
