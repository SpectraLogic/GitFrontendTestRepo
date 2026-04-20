/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service.api;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BeansRetrieverManager
{
    public < T extends SimpleBeanSafeToProxy > BeansRetriever< T > getRetriever( final Class< T > type );
}
