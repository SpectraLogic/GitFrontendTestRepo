/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.temp;

import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.temp.BlobAzureTargetToVerify;

final class BlobAzureTargetToVerifyServiceImpl
    extends BaseBlobTargetToVerifyService< BlobAzureTarget, BlobAzureTargetToVerify, SuspectBlobAzureTarget >
    implements BlobAzureTargetToVerifyService
{
    BlobAzureTargetToVerifyServiceImpl()
    {
        super( BlobAzureTarget.class, BlobAzureTargetToVerify.class, SuspectBlobAzureTarget.class );
    }
}
