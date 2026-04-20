/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.util.db.service.BaseService;

final class S3TargetServiceImpl
    extends BaseService< S3Target > implements S3TargetService
{
    S3TargetServiceImpl()
    {
        super( S3Target.class );
    }
}
