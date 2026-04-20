/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.service.BaseDatabaseBeansRetriever;
import com.spectralogic.util.exception.GenericFailure;

final class CountyServiceImpl extends BaseDatabaseBeansRetriever< County > implements CountyService
{
    CountyServiceImpl()
    {
        super( County.class, GenericFailure.NOT_FOUND );
    }
}
