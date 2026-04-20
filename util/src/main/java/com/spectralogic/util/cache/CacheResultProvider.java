/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.cache;

public interface CacheResultProvider< P, R >
{
    public R generateCacheResultFor( final P param );
}
