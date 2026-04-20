/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.S3TargetFailure;

final class S3TargetFailureServiceImpl
    extends BaseTargetFailureService< S3TargetFailure > implements S3TargetFailureService
{
    S3TargetFailureServiceImpl()
    {
        super( S3TargetFailure.class );
    }
}
