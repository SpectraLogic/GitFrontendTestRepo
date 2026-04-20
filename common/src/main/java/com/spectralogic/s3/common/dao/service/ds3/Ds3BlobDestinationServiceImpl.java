/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3BlobDestination;
import com.spectralogic.util.db.service.BaseService;

final class Ds3BlobDestinationServiceImpl
    extends BaseService<Ds3BlobDestination> implements Ds3BlobDestinationService
{
    Ds3BlobDestinationServiceImpl()
    {
        super( Ds3BlobDestination.class );
    }
}
