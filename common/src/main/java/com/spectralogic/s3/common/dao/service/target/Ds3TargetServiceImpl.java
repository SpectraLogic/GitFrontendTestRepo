/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.util.db.service.BaseService;

final class Ds3TargetServiceImpl
    extends BaseService< Ds3Target > implements Ds3TargetService
{
    Ds3TargetServiceImpl()
    {
        super( Ds3Target.class );
    }
}
