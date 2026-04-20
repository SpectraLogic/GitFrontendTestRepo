/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.util.db.service.BaseService;

final class AzureTargetServiceImpl
    extends BaseService< AzureTarget > implements AzureTargetService
{
    AzureTargetServiceImpl()
    {
        super( AzureTarget.class );
    }
}
