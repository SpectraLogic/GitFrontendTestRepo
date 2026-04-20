/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.HttpLogger;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.frmwrk.RequestProcessingThreadRenamer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;

public abstract class BaseServlet< S extends SimpleBeanSafeToProxy > extends HttpServlet
{
    protected BaseServlet( final RequestType ... requestTypesSupported )
    {
        m_supportedRequestTypes = CollectionFactory.toSet( requestTypesSupported );
    }
    

    @Override
    public final void doPost( final HttpServletRequest request, final HttpServletResponse response )
            throws ServletException, IOException
    {
        doInternal( RequestType.POST, request, response );
    }


    @Override
    public final void doPut( final HttpServletRequest request, final HttpServletResponse response ) 
            throws ServletException, IOException
    {
        doInternal( RequestType.PUT, request, response );
    }
    
    
    @Override
    public final void doGet( final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException
    {
        doInternal( RequestType.GET, request, response );
    }
    
    
    @Override
    public final void doDelete( final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException
    {
        doInternal( RequestType.DELETE, request, response );
    }
    
    
    @Override
    public final void doHead( final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException
    {
        doInternal( RequestType.HEAD, request, response );
    }
    
    
    private void doInternal(
            final RequestType requestType, 
            final HttpServletRequest request, 
            final HttpServletResponse response )
    {
        NUM_ACTIVE_REQUESTS.incrementAndGet();
        final Object duration = request.getAttribute( PROCESSING_TIME_DURATION_KEY );
        final Object requestHandler = request.getAttribute( PROCESSING_REQUEST_HANDLER );
        
        // https://wiki.apache.org/tomcat/FAQ/CharacterEncoding
        response.setCharacterEncoding( "UTF-8" );
        
        final RequestProcessingThreadRenamer threadRenamer =
                ( response.containsHeader( S3HeaderType.AMAZON_REQUEST_ID.getHttpHeaderName() ) )? 
                        new RequestProcessingThreadRenamer( 
                                Long.valueOf( response.getHeader( 
                                        S3HeaderType.AMAZON_REQUEST_ID.getHttpHeaderName() ) ).longValue() )
                        : null;
        if ( null != threadRenamer )
        {
            threadRenamer.run();
        }
        try
        {
            if ( !m_supportedRequestTypes.contains( requestType ) )
            {
                throw new UnsupportedOperationException(
                        "HTTP request type " + requestType 
                        + " is not supported by " + getClass().getName() + "." );
            }
            
            @SuppressWarnings( "unchecked" )
            final S params = (S)request.getAttribute( BaseServlet.class.getName() );
            provideResponse( params, requestType, request, response );

            int maxHeaderKeyLength = -1;
            final Map< String, String > headers = new HashMap<>();
            final Collection< String > headerNames = response.getHeaderNames();
            if ( null != headerNames )
            {
                for ( final String name : headerNames)
                {
                    headers.put( name, response.getHeader( name ) );
                    maxHeaderKeyLength = Math.max( maxHeaderKeyLength, name.length() );
                }
            }
            
            final char lc = '-';
            final String message = Platform.NEWLINE + Platform.NEWLINE + lc + " HTTP Request #" 
                    + response.getHeader( S3HeaderType.AMAZON_REQUEST_ID.getHttpHeaderName() ) 
                    + " Processed by " + requestHandler
                    + Platform.NEWLINE + "  " + lc + " Return Code: "
                    + String.valueOf( response.getStatus() )
                    + Platform.NEWLINE + "  " + lc + " Processing Time: " + duration;
            String verboseMessagePart = Platform.NEWLINE + "  " + lc + " HTTP Headers: ";
            if ( headers.isEmpty() )
            {
                verboseMessagePart += "<none>";
            }
            for ( final Map.Entry< String, String > header : headers.entrySet() )
            {
                verboseMessagePart += Platform.NEWLINE + "    ";
                for ( int i = header.getKey().length(); i < maxHeaderKeyLength + 2; ++i )
                {
                    verboseMessagePart += " ";
                }
                verboseMessagePart += header.getKey() + " | " + header.getValue();
            }
            verboseMessagePart += Platform.NEWLINE;
            
            LOG.info( message + Platform.NEWLINE );
            
            WireLogger.LOG.info( message + verboseMessagePart );
            String nar = String.valueOf( NUM_ACTIVE_REQUESTS.get() - 1 );
            while ( 3 > nar.length() )
            {
                nar = " " + nar;
            }
            HttpLogger.LOG.info( response.getStatus() + " |" + nar + " | " + duration );
        }
        catch ( final Exception ex )
        {
            LOG.error( getClass().getSimpleName() + " failed to provide a response to the client.", ex );
            response.setStatus( HttpServletResponse.SC_INTERNAL_SERVER_ERROR );
        }
        finally
        {
            NUM_ACTIVE_REQUESTS.decrementAndGet();
            if ( null != threadRenamer )
            {
                threadRenamer.shutdown();
            }
        }
    }
    
    
    protected abstract void provideResponse(
            final S params,
            final RequestType requestType, 
            final HttpServletRequest request, 
            final HttpServletResponse response ) throws Exception;
    
    
    final protected static void save( 
            final CommandExecutionParams params, 
            final SimpleBeanSafeToProxy state )
    {
        params.getModel().addAttribute( BaseServlet.class.getName(), state );
    }
    
    
    private final Set< RequestType > m_supportedRequestTypes;
    private final static Logger LOG = Logger.getLogger( BaseServlet.class );
    public final static String PROCESSING_REQUEST_HANDLER = BaseServlet.class.getName() + "-ProcessingRH";
    public final static String PROCESSING_TIME_DURATION_KEY = BaseServlet.class.getName() + "-ProcessingTD"; 
    public final static AtomicInteger NUM_ACTIVE_REQUESTS = new AtomicInteger( 0 );
}
