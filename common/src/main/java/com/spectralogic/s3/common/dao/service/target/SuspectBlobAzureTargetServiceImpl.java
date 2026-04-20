/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.service.suspectblob.BaseSuspectBlobTargetService;

final class SuspectBlobAzureTargetServiceImpl
    extends BaseSuspectBlobTargetService< SuspectBlobAzureTarget > implements SuspectBlobAzureTargetService
{
    SuspectBlobAzureTargetServiceImpl()
    {
        super( SuspectBlobAzureTarget.class );
    }
}
