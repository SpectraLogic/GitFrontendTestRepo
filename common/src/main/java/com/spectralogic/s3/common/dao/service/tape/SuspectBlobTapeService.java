/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface SuspectBlobTapeService
    extends BeansRetriever< SuspectBlobTape >, BeanCreator< SuspectBlobTape >
{
    void delete( final Set< UUID > ids );
}
