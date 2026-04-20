/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.suspectblob.BaseSuspectBlobTargetService;

final class SuspectBlobS3TargetServiceImpl
    extends BaseSuspectBlobTargetService< SuspectBlobS3Target > implements SuspectBlobS3TargetService
{
    SuspectBlobS3TargetServiceImpl()
    {
        super( SuspectBlobS3Target.class );
    }
}
