/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;

final class AzureTargetFailureServiceImpl
    extends BaseTargetFailureService< AzureTargetFailure > implements AzureTargetFailureService
{
    AzureTargetFailureServiceImpl()
    {
        super( AzureTargetFailure.class );
    }
}
