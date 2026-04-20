/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.LocalBlobDestination;
import com.spectralogic.util.db.service.BaseService;

final class LocalBlobDestinationServiceImpl
    extends BaseService<LocalBlobDestination> implements LocalBlobDestinationService
{
    LocalBlobDestinationServiceImpl()
    {
        super( LocalBlobDestination.class );
    }
}
