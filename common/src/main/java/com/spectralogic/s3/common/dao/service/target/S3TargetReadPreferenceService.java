/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.S3TargetReadPreference;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface S3TargetReadPreferenceService
    extends BeansRetriever< S3TargetReadPreference >,
            BeanCreator< S3TargetReadPreference >,
            BeanDeleter
{
    // empty
}
