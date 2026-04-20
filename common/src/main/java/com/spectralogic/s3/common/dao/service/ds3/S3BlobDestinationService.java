/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.S3BlobDestination;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface S3BlobDestinationService
    extends BeansRetriever<S3BlobDestination>,
            BeanCreator<S3BlobDestination>,
            BeanUpdater<S3BlobDestination>,
            BeanDeleter
{
    // empty
}
