/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.service.suspectblob.BaseSuspectBlobTargetService;

final class SuspectBlobDs3TargetServiceImpl
    extends BaseSuspectBlobTargetService< SuspectBlobDs3Target > implements SuspectBlobDs3TargetService
{
    SuspectBlobDs3TargetServiceImpl()
    {
        super( SuspectBlobDs3Target.class );
    }
}
