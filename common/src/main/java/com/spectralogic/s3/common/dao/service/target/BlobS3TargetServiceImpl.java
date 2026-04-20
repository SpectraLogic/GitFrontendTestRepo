/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;

final class BlobS3TargetServiceImpl
    extends BaseBlobTargetService< BlobS3Target, S3DataReplicationRule > implements BlobS3TargetService
{
    BlobS3TargetServiceImpl()
    {
        super( BlobS3Target.class, S3DataReplicationRule.class, DegradedBlob.S3_REPLICATION_RULE_ID );
    }
}
