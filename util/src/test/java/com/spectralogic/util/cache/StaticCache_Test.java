/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.cache;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class StaticCache_Test 
{
    @Test
    public void testConstructorNullNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new StaticCache<>( null );
            }
        } );
    }
    
    
    @Test
    public void testCacheCachesResults()
    {
        final AtomicInteger callCount = new AtomicInteger();
        final CacheResultProvider< Integer, String > provider = new CacheResultProvider< Integer, String >()
        {
            public String generateCacheResultFor( final Integer param )
            {
                callCount.addAndGet( 1 );
                if ( null == param )
                {
                    return null;
                }
                return "string" + param;
            }
        };
        
        final StaticCache< Integer, String > cache = new StaticCache<>( provider );
        assertEquals(
                "string1",
                cache.get( Integer.valueOf( 1 ) ),
                "Shoulda cached result."
                 );
        assertEquals(
                1,
                callCount.get(),
                "Shoulda cached result."
                );
        assertEquals(
                "string1",
                cache.get( Integer.valueOf( 1 ) ),
                "Shoulda cached result."
                );
        assertEquals(
                1,
                callCount.get(),
                "Shoulda cached result."
                 );
        assertEquals(
                "string3",
                cache.get( Integer.valueOf( 3 ) ),
                "Shoulda cached result."
                );
        assertEquals(
                2,
                callCount.get(),
                "Shoulda cached result."
                 );
        assertEquals(
                null,
                cache.get( null ),
                "Shoulda cached result."
               );
        assertEquals(
                3,
                callCount.get(),
                "Shoulda cached result."
                 );
        assertEquals(
                null,
                cache.get( null ),
                "Shoulda cached result."
                 );
        assertEquals(
                3,
                callCount.get(),
                "Shoulda cached result."
                 );
    }
    
    
    @Test
    public void testDefensiveCopyReturnedWhenGettingCacheResult()
    {
        final CacheResultProvider< Object, Set< String > > provider = 
                new CacheResultProvider< Object, Set< String > >()
                {
                    public Set< String > generateCacheResultFor( final Object param )
                    {
                        return CollectionFactory.toSet( "a", "b" );
                    }
                };
        final StaticCache< Object, Set< String > > cache = new StaticCache<>( provider );
        assertTrue(cache.get( null ).remove( "a" ), "Shoulda defensively copied set.");
        assertTrue(cache.get( null ).remove( "a" ), "Shoulda defensively copied set.");
        assertTrue(cache.get( null ).remove( "a" ), "Shoulda defensively copied set.");
    }
}
