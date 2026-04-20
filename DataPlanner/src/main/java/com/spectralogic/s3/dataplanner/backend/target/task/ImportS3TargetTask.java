/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.ImportS3TargetDirective;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.S3TargetBucketName;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.ImportS3TargetDirectiveService;
import com.spectralogic.s3.common.dao.service.target.S3TargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.S3TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class ImportS3TargetTask
    extends BaseImportTargetTask
    < S3Target, 
      BlobS3Target, 
      ImportS3TargetDirective,
      S3ConnectionFactory, 
      S3TargetBucketName >
{
    public ImportS3TargetTask( final BlobStoreTaskPriority priority,
                               final UUID targetId,
                               final DiskManager diskManager,
                               final JobProgressManager jobProgressManager,
                               final BeansServiceManager serviceManager,
                               final S3ConnectionFactory connectionFactory )
    {
        super( S3Target.class,
                S3TargetBucketNameService.class, 
                S3TargetFailureService.class, 
                BlobS3Target.class,
                ImportS3TargetDirectiveService.class, 
                S3TargetBucketName.class,
                diskManager,
                jobProgressManager,
                serviceManager,
                connectionFactory,
                priority, 
                targetId );
    }

}
