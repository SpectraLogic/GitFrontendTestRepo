/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.AzureTargetReadPreference;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface AzureTargetReadPreferenceService
    extends BeansRetriever< AzureTargetReadPreference >,
            BeanCreator< AzureTargetReadPreference >,
            BeanDeleter
{
    // empty
}
