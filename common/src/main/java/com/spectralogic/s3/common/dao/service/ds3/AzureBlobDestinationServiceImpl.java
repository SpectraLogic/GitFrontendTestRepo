/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.AzureBlobDestination;
import com.spectralogic.util.db.service.BaseService;

final class AzureBlobDestinationServiceImpl
    extends BaseService<AzureBlobDestination> implements AzureBlobDestinationService
{
    AzureBlobDestinationServiceImpl()
    {
        super( AzureBlobDestination.class );
    }
}
