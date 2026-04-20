/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.cache.throttle;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.log4j.Logger;

import junit.framework.TestCase;


/**
 * Works in conjunction with the primary ThreadLeakHunter in 'util' to detect
 * and when possible prevent biz state (resource) leaks by test methods, leaks
 * that propogate into test methods that run later, invalidating the later
 * methods' biz state indpendence from test methods that ran earlier and leaked
 * resources.
 * 
 * This aspect only exsits becuase neither the source nor class files for
 * DataPlanner's CacheAllocationThrottle are available to the 'util' command
 * line build (e.g. moving this aspect's code to the 'util' TLH will not cause
 * problems in Eclipse).
 * 
 * Do not change this aspect without first understanding the 'util' TLH.
 * 
 * Compile speed info to keep in mind while changing or extending this aspect:
 * 
 * https://wiki.eclipse.org/AspectJBuildSpeed
 */
public final aspect ThreadLeakHunter
{
    /**
     *  This aspect has the lowest precedence WRT its placement along side other
     *  aspects at join points. Eliminating this line, at a minium, causes the
     *  AspectJ weaver to issue many avoidable log warnings, thus slowing it
     *  down, and at a maximum can cause logic errors in aspect weaving.
     */
    declare precedence: *, com.spectralogic.s3.dataplanner.cache.throttle.ThreadLeakHunter;
    
    pointcut testMethod( TestCase tc ) : target( tc ) &&
                                         execution( public void test*() );
    
    @SuppressWarnings( "rawtypes" )
    after( @SuppressWarnings( "unused" ) final TestCase tc ) : testMethod( tc )
    {
        try
        {
            ((Map)CACHE_ALLOC_THROTTLES.get(
                                   (CacheAllocationThrottle)null )).clear();
        }
        catch ( final IllegalArgumentException | IllegalAccessException ex )
        {
            LOG.error( ex );
            throw new RuntimeException( ex );
        }
    }
    
    
    private static final Logger LOG = Logger.getLogger( ThreadLeakHunter.class );
    
    private static final Field CACHE_ALLOC_THROTTLES;
    static
    {
        try
        {
            CACHE_ALLOC_THROTTLES = CacheAllocationThrottleProvider.class
                                               .getDeclaredField( "THROTTLES" );
            CACHE_ALLOC_THROTTLES.setAccessible( true );
        }
        catch ( final NoSuchFieldException | SecurityException ex )
        {
            LOG.error( ex );
            throw new RuntimeException( ex );
        }
    }
}
