/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.cache;

import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

/**
 * A static cache that always returns the same result given a particular request.  This type of cache lazily
 * calculates a result when needed, then retains that result forever.
 */
public final class StaticCache< P, R >
{
    public StaticCache( final CacheResultProvider< P, R > resultProvider )
    {
        m_resultProvider = resultProvider;
        Validations.verifyNotNull( "Result provider", m_resultProvider );
    }
    
    
    synchronized public R get( final P param )
    {
        if ( m_cache.containsKey( param ) )
        {
            return CollectionFactory.getDefensiveCopy( m_cache.get( param ) );
        }

        m_cache.put( 
                param, 
                CollectionFactory.getDefensiveCopy( m_resultProvider.generateCacheResultFor( param ) ) );
        return get( param );
    }
    
    
    private final Map< P, R > m_cache = new HashMap<>();
    private final CacheResultProvider< P, R > m_resultProvider;
}
