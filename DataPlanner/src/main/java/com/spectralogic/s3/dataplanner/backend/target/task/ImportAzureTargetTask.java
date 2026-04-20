/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ImportAzureTargetDirective;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.target.AzureTargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.AzureTargetFailureService;
import com.spectralogic.s3.common.dao.service.target.ImportAzureTargetDirectiveService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class ImportAzureTargetTask 
  extends BaseImportTargetTask
  < AzureTarget, 
    BlobAzureTarget, 
    ImportAzureTargetDirective,
    AzureConnectionFactory,
    AzureTargetBucketName >
{
    public ImportAzureTargetTask(final BlobStoreTaskPriority priority,
                                 final UUID targetId,
                                 final DiskManager diskManager,
                                 final JobProgressManager jobProgressManager,
                                 final BeansServiceManager serviceManager,
                                 final AzureConnectionFactory connectionFactory)
    {
        super( AzureTarget.class,
               AzureTargetBucketNameService.class, 
               AzureTargetFailureService.class, 
               BlobAzureTarget.class,
               ImportAzureTargetDirectiveService.class, 
               AzureTargetBucketName.class,
               diskManager,
               jobProgressManager,
               serviceManager,
               connectionFactory,
               priority,
               targetId );
    }

}
