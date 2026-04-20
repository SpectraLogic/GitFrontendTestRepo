/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.Ds3TargetReadPreference;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface Ds3TargetReadPreferenceService
    extends BeansRetriever< Ds3TargetReadPreference >,
            BeanCreator< Ds3TargetReadPreference >,
            BeanDeleter
{
    // empty
}
