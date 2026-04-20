/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.S3TargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.S3TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.frmwrk.TargetWriteDirective;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class WriteChunkToS3TargetTask 
    extends BaseWriteChunkToPublicCloudTask
            < S3Target, BlobS3Target, SuspectBlobS3Target, S3DataReplicationRule, S3BlobDestination, S3ConnectionFactory >
{
    public WriteChunkToS3TargetTask(
            final JobProgressManager jobProgressManager,
            final DiskManager diskManager,
            final BeansServiceManager serviceManager,
            final S3ConnectionFactory connectionFactory,
            final TargetWriteDirective< S3Target, S3BlobDestination> writeDirective )
    {
        super( S3Target.class,
                S3TargetBucketNameService.class,
                S3TargetFailureService.class,
                BlobS3Target.class,
                SuspectBlobS3Target.class,
                S3DataReplicationRule.class,
                S3BlobDestination.class,
                writeDirective,
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory );
    }


    @Override
    protected Object getInitialDataPlacement( final Bucket bucket, final S3DataReplicationRule rule )
    {
        return rule.getInitialDataPlacement();
    }
}
