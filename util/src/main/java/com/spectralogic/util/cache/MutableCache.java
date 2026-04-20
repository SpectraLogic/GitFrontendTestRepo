/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;

/**
 * A mutable cache that may return different results for the same input over time.
 */
public final class MutableCache< P, R >
{
    public MutableCache( 
            final long timeToRetainCacheResultInMillis,
            final CacheResultProvider< P, R > resultProvider )
    {
        m_resultProvider = resultProvider;
        m_timeToRetainCacheResultInMillis = timeToRetainCacheResultInMillis;
        Validations.verifyNotNull( "Result provider", m_resultProvider );
    }
    
    
    synchronized public R get( final P param )
    {
        expellStaleCacheEntries();
        if ( m_cache.containsKey( param ) )
        {
            return CollectionFactory.getDefensiveCopy( m_cache.get( param ).m_value );
        }

        final Duration duration = new Duration();
        m_cache.put( 
                param, 
                new CacheResult<>(
                        duration,
                        CollectionFactory.getDefensiveCopy( 
                                m_resultProvider.generateCacheResultFor( param ) ) ) );
        return get( param );
    }
    
    
    synchronized public void clearCacheContents()
    {
        m_cache.clear();
    }
    
    
    private void expellStaleCacheEntries()
    {
        if ( m_durationSinceLastStaleCacheEntryCleanup.getElapsedMillis() 
                < m_timeToRetainCacheResultInMillis / 2 )
        {
            return;
        }
        
        final Set< P > removes = new HashSet<>();
        for ( final Map.Entry< P, CacheResult< R > > e : m_cache.entrySet() )
        {
            if ( e.getValue().m_cachedDuration.getElapsedMillis() 
                    > e.getValue().m_millisRequiredToCacheResult + m_timeToRetainCacheResultInMillis )
            {
                removes.add( e.getKey() );
            }
        }
        for ( final P remove : removes )
        {
            m_cache.remove( remove );
        }
        
        m_durationSinceLastStaleCacheEntryCleanup = new Duration();
    }
    
    
    private final static class CacheResult< V >
    {
        private CacheResult( final Duration durationRequiredToCacheResult, final V value )
        {
            m_value = value;
            m_millisRequiredToCacheResult = durationRequiredToCacheResult.getElapsedMillis();
        }
        
        private final V m_value;
        private final long m_millisRequiredToCacheResult;
        private final Duration m_cachedDuration = new Duration();
    } // end inner class def
    
    
    private Duration m_durationSinceLastStaleCacheEntryCleanup = new Duration();
    private final Map< P, CacheResult< R > > m_cache = new HashMap<>();
    private final CacheResultProvider< P, R > m_resultProvider;
    private final long m_timeToRetainCacheResultInMillis;
}
