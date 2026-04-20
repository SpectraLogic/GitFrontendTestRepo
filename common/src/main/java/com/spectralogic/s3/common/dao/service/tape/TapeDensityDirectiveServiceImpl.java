/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective;
import com.spectralogic.util.db.service.BaseService;

final class TapeDensityDirectiveServiceImpl
    extends BaseService< TapeDensityDirective > implements TapeDensityDirectiveService
{
    TapeDensityDirectiveServiceImpl()
    {
        super( TapeDensityDirective.class );
    }
}
