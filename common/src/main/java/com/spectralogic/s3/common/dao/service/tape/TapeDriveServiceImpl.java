/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.util.db.service.BaseService;

final class TapeDriveServiceImpl extends BaseService< TapeDrive > implements TapeDriveService
{
    TapeDriveServiceImpl()
    {
        super( TapeDrive.class );
    }
}
