/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.LocalBlobDestination;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface LocalBlobDestinationService
    extends BeansRetriever<LocalBlobDestination>, BeanDeleter, BeanUpdater<LocalBlobDestination>
{
    void create( final Set<LocalBlobDestination> targets );
}
