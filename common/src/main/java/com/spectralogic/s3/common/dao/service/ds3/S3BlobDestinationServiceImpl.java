/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.S3BlobDestination;
import com.spectralogic.util.db.service.BaseService;

final class S3BlobDestinationServiceImpl
    extends BaseService<S3BlobDestination> implements S3BlobDestinationService
{
    S3BlobDestinationServiceImpl()
    {
        super( S3BlobDestination.class );
    }
}
