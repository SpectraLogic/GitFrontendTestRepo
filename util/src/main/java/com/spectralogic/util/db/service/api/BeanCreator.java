/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service.api;

import java.util.Set;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BeanCreator< T extends SimpleBeanSafeToProxy >
{
    void create( final T bean );
    
    
    void create( final Set< T > beans );
}
