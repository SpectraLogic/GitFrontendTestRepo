/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface SuspectBlobS3TargetService
    extends BeansRetriever< SuspectBlobS3Target >, BeanCreator< SuspectBlobS3Target >, BeanDeleter
{
    void delete( final Set< UUID > ids );
}
