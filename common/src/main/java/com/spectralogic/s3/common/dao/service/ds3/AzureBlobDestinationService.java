/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.AzureBlobDestination;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface AzureBlobDestinationService
    extends BeansRetriever<AzureBlobDestination>,
            BeanCreator<AzureBlobDestination>,
            BeanUpdater<AzureBlobDestination>,
            BeanDeleter
{
    // empty
}
