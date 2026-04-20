/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.temp;

import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.domain.temp.BlobS3TargetToVerify;

final class BlobS3TargetToVerifyServiceImpl 
    extends BaseBlobTargetToVerifyService< BlobS3Target, BlobS3TargetToVerify, SuspectBlobS3Target >
    implements BlobS3TargetToVerifyService
{
    BlobS3TargetToVerifyServiceImpl()
    {
        super( BlobS3Target.class, BlobS3TargetToVerify.class, SuspectBlobS3Target.class );
    }
}
