/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.spectralogic.s3.server.handler.find.RequestHandlerProvider;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.system.GetRequestHandlersRequestHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.predicate.UnaryPredicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class AllRequestHandlersIntegration_Test
{
    @Test
    public void testEveryRequestHandlerHasDocumentation()
    {
        final InputStream propertiesIs = getClass().getResourceAsStream( "/requesthandlers.props" );
        final Properties properties = new Properties();
        try
        {
            properties.load( propertiesIs );
            propertiesIs.close();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to load request handler documentation.", ex );
        }
        
        final Map< String, String > props = new HashMap<>();
        for ( final Map.Entry< Object, Object > e : properties.entrySet() )
        {
            props.put( (String)e.getKey(), (String)e.getValue() );
        }

        final Set< String > missingKeys = new TreeSet<>();
        final Set< String > failures = new TreeSet<>();
        for ( final RequestHandler handler : RequestHandlerProvider.getAllRequestHandlers() )
        {
            if ( !props.containsKey( handler.getClass().getName() + ".documentation" ) )
            {
                missingKeys.add( handler.getClass().getName() );
                failures.add( "No documentation exists for handler: " + handler.getClass().getName() );
            }
            if ( !props.containsKey( handler.getClass().getName() + ".version" ) )
            {
                failures.add( "No version exists for handler: " + handler.getClass().getName() );
            }
            props.remove( handler.getClass().getName() + ".documentation" );
            props.remove( handler.getClass().getName() + ".version" );
        }

        final PrintStream out = System.out;
        if ( !missingKeys.isEmpty() )
        {
            out.println(
                    "You may use the stubbed-out documentation below.  " 
                    + "Just copy and paste into requesthandlers.props and fill out the documentation part." );
        }
        for ( final String key : missingKeys )
        {
            out.println( "" );
            out.println( key + ".version=1" );
            out.println( key + ".documentation=\\" );
            out.println( "" );
        }
        for ( final String prop : props.keySet() )
        {
            failures.add(
               "Extra entry found with no request handler reference: " + prop + " -> " + props.get( prop ) );
        }
        if ( !failures.isEmpty() )
        {
            Assertions.fail( failures.size() + " violations: " + failures );
        }
    }
    
    
    @Test
    public void testEveryRequestHandlerHasTests()
    {
        final Set< String > violations = new HashSet<>();
        
        for ( final Map.Entry< Class< ? >, Class< ? > > e : getTestClassesForRequestHandlers().entrySet() )
        {
            if ( null != e.getValue() )
            {
                continue;
            }
            violations.add( e.getKey().getName() );
        }
        
        if ( !violations.isEmpty() )
        {
            Assertions.fail( "Tests are missing for " + violations.size() + " request handlers: " + violations );
        }
    }
    
    
    @Test
    public void testEveryRequestHandlerHasExampleResponseDocumentation()
    {
        final Set< String > requestHandlersWithDocumentation = new HashSet<>();
        final InputStream srPropertiesIs = 
                GetRequestHandlersRequestHandler.class.getResourceAsStream(
                        "/requesthandlerresponses.props" );
        final Properties srProperties = new Properties();
        try
        {
            srProperties.load( srPropertiesIs );
            srPropertiesIs.close();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to load request handler documentation.", ex );
        }
        
        for ( final Object key : srProperties.keySet() )
        {
            final String fullKey = key.toString();
            final String longKey = fullKey.substring( 0, fullKey.lastIndexOf( '.' ) );
            requestHandlersWithDocumentation.add( longKey.substring( 0, longKey.lastIndexOf( '.' ) ) );
        }
        
        final Set< String > violations = new HashSet<>();
        
        for ( final Map.Entry< Class< ? >, Class< ? > > e : getTestClassesForRequestHandlers().entrySet() )
        {
            if ( requestHandlersWithDocumentation.contains( e.getKey().getName() )
                    || null != e.getKey().getAnnotation( ExcludeRequestHandlerResponseDocumentation.class ) )
            {
                continue;
            }
            violations.add( e.getKey().getName() );
        }
        
        if ( !violations.isEmpty() )
        {
            Assertions.fail( "Example response documentation is missing for "
                  + violations.size() + " request handlers: " + violations
                  + " (Please see the bottom of " + MockHttpRequestDriver.class.getSimpleName() 
                  + " for how to generate this documentation and fix this test failure)" );
        }
    }
    
    
    private Map< Class< ? >, Class< ? > > getTestClassesForRequestHandlers()
    {
        final Map< Class< ? >, Class< ? > > retval = new HashMap<>();
        final Map< String, Class< ? > > testedRequestHandlers = new HashMap<>();
        for ( final RequestHandler handler : RequestHandlerProvider.getAllRequestHandlers() )
        {
            retval.put( handler.getClass(), null );
            testedRequestHandlers.put( handler.getClass().getName() + "_Test", handler.getClass() );
        }

        final PackageContentFinder finder = new PackageContentFinder(
                getClass().getPackage().getName(), AllRequestHandlersIntegration_Test.class, null );
        final Set< Class< ? > > testClasses = finder.getClasses( new UnaryPredicate< Class<?> >()
        {
            public boolean test( final Class< ? > element )
            {
                return ( testedRequestHandlers.containsKey( element.getName() ) );
            }
        } );
        for ( final Class< ? > testClass : testClasses )
        {
            retval.put( testedRequestHandlers.get( testClass.getName() ), testClass );
        }
        
        return retval;
    }
}
