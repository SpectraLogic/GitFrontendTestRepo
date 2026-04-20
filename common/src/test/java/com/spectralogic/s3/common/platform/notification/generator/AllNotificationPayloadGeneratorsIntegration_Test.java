/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.io.IOUtils;

import com.spectralogic.s3.common.platform.notification.domain.payload.JobCompletedNotificationPayload;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.predicate.UnaryPredicate;

public final class AllNotificationPayloadGeneratorsIntegration_Test 
{
    @Test
    public void testEveryGeneratorHasTests()
    {
        final PackageContentFinder generatorFinder = new PackageContentFinder( 
                getClass().getPackage().getName(), JobCompletedNotificationPayloadGenerator.class, null );
        final Set< Class< ? > > generators = generatorFinder.getClasses( new UnaryPredicate< Class< ? > >()
        {
            public boolean test( final Class< ? > element )
            {
                return NotificationPayloadGenerator.class.isAssignableFrom( element );
            }
        } );
        
        final Set< String > expectedTestClasses = new HashSet<>();
        for ( final Class< ? > generator : generators )
        {
            expectedTestClasses.add( generator.getName() + "_Test" );
        }
        if ( expectedTestClasses.isEmpty() )
        {
            fail( "Didn't find any generators to require tests for." );
        }
        
        final PackageContentFinder testFinder = new PackageContentFinder( 
                getClass().getPackage().getName(), 
                AllNotificationPayloadGeneratorsIntegration_Test.class,
                null );
        for ( final Class< ? > clazz : testFinder.getClasses( null ) )
        {
            expectedTestClasses.remove( clazz.getName() );
        }
        
        if ( expectedTestClasses.isEmpty() )
        {
            return;
        }
        fail( "Event generators did not have any tests.  Please write tests: " + expectedTestClasses );
    }
    
    
    @Test
    public void testEveryNotificationPayloadTypeHasDocumentation()
    {
        final PackageContentFinder payloadFinder = new PackageContentFinder( 
                JobCompletedNotificationPayload.class.getPackage().getName(),
                JobCompletedNotificationPayload.class,
                null );
        final Set< Class< ? > > payloads = payloadFinder.getClasses( new UnaryPredicate< Class< ? > >()
        {
            public boolean test( final Class< ? > element )
            {
                return NotificationPayload.class.isAssignableFrom( element );
            }
        } );
        
        final Set< String > expectedPayloads = new HashSet<>();
        for ( final Class< ? > payload : payloads )
        {
            expectedPayloads.add( payload.getSimpleName() );
        }
        if ( expectedPayloads.isEmpty() )
        {
            fail( "Didn't find any payloads to require docs for." );
        }
        
        final String docs;
        try
        {
            final InputStream p4FileIs = NotificationPayload.class.getResourceAsStream( 
                    "/" + NotificationPayloadTracker.FILE_NAME );
            docs = IOUtils.toString( p4FileIs, Charset.defaultCharset() );
            p4FileIs.close();
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        
        for ( final String expected : expectedPayloads )
        {
            if ( !docs.contains( expected ) )
            {
                fail( "Notification payload not documented: " + expected + " (see "
                      + NotificationPayloadTracker.class.getName() 
                      + " for instructions to re-generate docs)" );
            }
        }
    }
}
