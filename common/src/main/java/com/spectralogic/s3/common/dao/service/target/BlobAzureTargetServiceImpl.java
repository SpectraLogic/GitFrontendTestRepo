/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;

final class BlobAzureTargetServiceImpl
    extends BaseBlobTargetService< BlobAzureTarget, AzureDataReplicationRule >
    implements BlobAzureTargetService
{
    BlobAzureTargetServiceImpl()
    {
        super( BlobAzureTarget.class, 
                AzureDataReplicationRule.class,
                DegradedBlob.AZURE_REPLICATION_RULE_ID );
    }
}
