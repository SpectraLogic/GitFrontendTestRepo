/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.AzureTargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.AzureTargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class WriteChunkToAzureTargetTask
    extends BaseWriteChunkToPublicCloudTask
    < AzureTarget, BlobAzureTarget, SuspectBlobAzureTarget, AzureDataReplicationRule, AzureBlobDestination, AzureConnectionFactory >
{

    public WriteChunkToAzureTargetTask(
            final JobProgressManager jobProgressManager,
            final DiskManager diskManager,
            final BeansServiceManager serviceManager,
            final AzureConnectionFactory connectionFactory,
            final TargetWriteDirective< AzureTarget, AzureBlobDestination> writeDirective )
    {
        super( AzureTarget.class,
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class,
                BlobAzureTarget.class,
                SuspectBlobAzureTarget.class,
                AzureDataReplicationRule.class,
                AzureBlobDestination.class,
                writeDirective,
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory );
    }


    @Override
    protected Object getInitialDataPlacement( final Bucket bucket, final AzureDataReplicationRule rule )
    {
        return null;
    }
}
