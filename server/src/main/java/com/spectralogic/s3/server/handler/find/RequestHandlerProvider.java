/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.find;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.s3.server.handler.reqhandler.RequestHandler;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.predicate.UnaryPredicate;

/**
 * Finds and provides all the request handlers defined dynamically and reflectively.
 */
public final class RequestHandlerProvider
{
    public static Set< RequestHandler > getAllRequestHandlers()
    {
        synchronized ( LOCK )
        {
            if ( null != s_cache )
            {
                return new HashSet<>( s_cache );
            }
            
            s_cache = loadAllRequestHandlers();
            return getAllRequestHandlers();
        }
    }
    
    
    private static Set< RequestHandler > loadAllRequestHandlers()
    {
        final Set< RequestHandler > retval = new HashSet<>();
        final Duration duration = new Duration();
        final PackageContentFinder requestHandlerFinder = new PackageContentFinder(
                RequestHandler.class.getPackage().getName(),
                RequestHandler.class,
                null );
        final Set< Class< ? > > requestHandlerClasses = requestHandlerFinder.getClasses(
                new UnaryPredicate< Class<?> >()
                {
                    public boolean test( final Class< ? > element )
                    {
                        return ( RequestHandler.class.isAssignableFrom( element ) 
                                && !Modifier.isAbstract( element.getModifiers() ) );
                    }
                } );
        if ( requestHandlerClasses.isEmpty() )
        {
            throw new RuntimeException( "Could not find any request handlers." );
        }
        
        try
        {
            for ( final Class< ? > clazz : requestHandlerClasses )
            {
                retval.add( (RequestHandler)clazz.newInstance() );
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to load request handlers.", ex );
        }
        
        LOG.info( "Loaded " + retval.size() + " request handlers in " + duration + "." );
        return retval;
    }
    
    
    private static Set< RequestHandler > s_cache;
    private static final Object LOCK = new Object();
    private final static Logger LOG = Logger.getLogger( RequestHandlerProvider.class );
}
