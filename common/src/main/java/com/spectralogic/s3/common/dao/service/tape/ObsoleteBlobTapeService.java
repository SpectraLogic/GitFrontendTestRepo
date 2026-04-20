/*******************************************************************************
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface ObsoleteBlobTapeService
        extends BeansRetriever< ObsoleteBlobTape >, BeanCreator< ObsoleteBlobTape >
{
    void delete( final Set< UUID > ids );
}
