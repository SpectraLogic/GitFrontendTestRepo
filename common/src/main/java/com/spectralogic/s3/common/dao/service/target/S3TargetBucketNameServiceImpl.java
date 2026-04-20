/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.S3TargetBucketName;

final class S3TargetBucketNameServiceImpl
    extends BaseTargetBucketNameService< S3TargetBucketName > 
    implements S3TargetBucketNameService
{
    S3TargetBucketNameServiceImpl()
    {
        super( S3TargetBucketName.class );
    }
}
