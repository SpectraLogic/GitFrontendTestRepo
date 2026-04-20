/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface TapeDensityDirectiveService 
    extends BeansRetriever< TapeDensityDirective >, BeanCreator< TapeDensityDirective >, BeanDeleter
{
    // empty
}
