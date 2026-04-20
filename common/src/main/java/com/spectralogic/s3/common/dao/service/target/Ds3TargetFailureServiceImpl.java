/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure;

final class Ds3TargetFailureServiceImpl
    extends BaseTargetFailureService< Ds3TargetFailure > implements Ds3TargetFailureService
{
    Ds3TargetFailureServiceImpl()
    {
        super( Ds3TargetFailure.class );
    }
}
