/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.WireLogger;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.http.HttpResponseFormatType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.marshal.Marshalable;
import com.spectralogic.util.marshal.XmlMarshaler;
import com.spectralogic.util.mock.InterfaceProxyFactory;

/**
 * Servlet that serves up beans as a response
 */
public final class BeanServlet extends BaseServlet< BeanServletParams >
{
    public BeanServlet()
    {
        super( RequestType.values() );
    }
    
    
    @Override
    protected void provideResponse(
            final BeanServletParams params,
            final RequestType requestType,
            final HttpServletRequest request,
            final HttpServletResponse response ) throws IOException
    {
        final HttpResponseFormatType responseType = HttpResponseFormatType.valueOf( 
                params.getRequestPath(),
                params.getContentType() );
        
        // Set the status before we start writing data because the status is the
        // first part of an HTTP request and setStatus won't do anything if data
        // has already been written.
        response.setStatus( params.getHttpResponseCode() );
        if ( null == params.getResponse() )
        {
            LOG.info( "There was no response to write.  The HTTP response code will be the only feedback." );
            return;
        }
        
        LOG.info( "Pushing bean result to client (response type " + responseType + ")..." );
        final BeanServletResponseListener responseTypeListener = s_responseListener;
        if ( null != responseTypeListener )
        {
            responseTypeListener.responseReturned( params.getResponse() );
        }
        
        final OutputStream os = response.getOutputStream();
        final String dataTag = ( null == params.getDataTag() ) ? 
                null : params.getNamingConvention().convert( params.getDataTag() );
        
        final String result;
        String prettyResult = "";
        response.setCharacterEncoding( "UTF-8" );
        if ( HttpResponseFormatType.JSON == responseType )
        {
            response.addHeader(
                    S3HeaderType.CONTENT_TYPE.getHttpHeaderName(),
                    "text/json" );
            if ( null != params.getResponse() && String.class == params.getResponse().getClass() )
            {
                result = (String)params.getResponse();
                prettyResult = result;
            }
            else
            {
                result = getJsonResponse( params.getResponse(), dataTag, params.getNamingConvention() );
                if ( WireLogger.LOG.isInfoEnabled() )
                {
                    prettyResult = JsonMarshaler.formatPretty( result );
                }
            }
        }
        else
        {
            response.addHeader(
                    S3HeaderType.CONTENT_TYPE.getHttpHeaderName(),
                    "text/xml" );
            if ( null != params.getResponse() && String.class == params.getResponse().getClass() )
            {
                result = (String)params.getResponse();
                prettyResult = result;
            }
            else
            {
                result = getXmlResponse( params.getResponse(), dataTag, params.getNamingConvention() );
                if ( WireLogger.LOG.isInfoEnabled() )
                {
                    prettyResult = XmlMarshaler.formatPretty( result );
                }
            }
        }
        
        WireLogger.LOG.info(
                "Response to client: " + Platform.NEWLINE 
                + sanitize(
                        LogUtil.getShortVersion( prettyResult ), 
                        getStringsToSanitize( params.getResponse() ) )
                + Platform.NEWLINE );
        os.write( result.getBytes() );
    }
    
    
    private String sanitize( final String original, final Set< String > stringsToSanitize )
    {
        String retval = original;
        for ( final String sts : stringsToSanitize )
        {
            retval = retval.replace( sts, "{CONCEALED}" );
        }
        return retval;
    }
    
    
    private Set< String > getStringsToSanitize( final Object o )
    {
        final Set< String > retval = new HashSet<>();
        if ( null == o )
        {
            return retval;
        }
        if ( Collection.class.isAssignableFrom( o.getClass() ) )
        {
            @SuppressWarnings( "unchecked" )
            final Collection< Object > collection = (Collection< Object >)o;
            for ( final Object e : collection )
            {
                retval.addAll( getStringsToSanitize( e ) );
            }
        }
        if ( o.getClass().isArray() )
        {
            for ( int i = 0; i < Array.getLength( o ); ++i )
            {
                retval.addAll( getStringsToSanitize( Array.get( o, i ) ) );
            }
        }
        if ( !Marshalable.class.isAssignableFrom( o.getClass() ) )
        {
            return retval;
        }
        
        for ( final String prop : BeanUtils.getPropertyNames( o.getClass() ) )
        {
            final Object value;
            final Method reader = BeanUtils.getReader( o.getClass(), prop );
            try
            {
                value = reader.invoke( o );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( 
                        "Failed to get " + prop + " on " + o.getClass().getSimpleName() + ".", ex );
            }
            if ( null != reader.getAnnotation( Secret.class ) && null != value )
            {
                retval.add( value.toString() );
            }
            retval.addAll( getStringsToSanitize( value ) );
        }
        
        return retval;
    }
    
    
    /**
     * Package private for testing purposes only.
     */
    String getXmlResponse( final Object results, final String dataTag, final NamingConventionType nc )
    {
        final String encodedResults = 
                ( null != results && Marshalable.class.isAssignableFrom( results.getClass() ) ) ?
                        ((Marshalable)results).toXml( nc )
                        : XmlMarshaler.marshal( results, nc );
        String topLevelAttributes = "";
        if ( null != results && Marshalable.class.isAssignableFrom( results.getClass() ) )
        {
            topLevelAttributes = XmlMarshaler.buildAttributesText(
                    XmlMarshaler.getAttributes( MarshalUtil.getMarshalableElements( results ) ),
                    nc );
        }
        return ( null == dataTag ) ? 
                encodedResults 
                : "<" + dataTag + topLevelAttributes + ">" + encodedResults + "</" + dataTag + ">";
    }
    

    /**
     * Package private for testing purposes only.
     */
    String getJsonResponse( final Object results, final String dataTag, final NamingConventionType nc )
    {
        final String encodedResults = 
                ( null != results && Marshalable.class.isAssignableFrom( results.getClass() ) ) ?
                        ((Marshalable)results).toJson( nc )
                        : JsonMarshaler.marshal( results, nc );
        return ( null == dataTag )
                ? encodedResults
                : "{\"" + dataTag + "\":" + encodedResults + "}";
    }
    
    
    public static ServletResponseStrategy serviceCreate(
            final CommandExecutionParams params,
            final Object resultToProvideToClient )
    {
        return serviceRequest(
                params, 
                ( null == resultToProvideToClient ) ? 
                        HttpServletResponse.SC_NO_CONTENT 
                        : HttpServletResponse.SC_CREATED,
                resultToProvideToClient );
    }
    
    
    public static ServletResponseStrategy serviceModify(
            final CommandExecutionParams params,
            final Object resultToProvideToClient )
    {
        return serviceRequest(
                params, 
                ( null == resultToProvideToClient ) ? 
                        HttpServletResponse.SC_NO_CONTENT 
                        : HttpServletResponse.SC_OK,
                resultToProvideToClient );
    }
    
    
    public static ServletResponseStrategy serviceDelete(
            final CommandExecutionParams params,
            final Object resultToProvideToClient )
    {
        return serviceRequest(
                params, 
                ( null == resultToProvideToClient ) ? 
                        HttpServletResponse.SC_NO_CONTENT 
                        : HttpServletResponse.SC_OK,
                resultToProvideToClient );
    }
    
    
    public static ServletResponseStrategy serviceGet(
            final CommandExecutionParams params,
            final Object resultToProvideToClient )
    {
        return serviceRequest(
                params, 
                HttpServletResponse.SC_OK,
                resultToProvideToClient );
    }
    
    
    public static ServletResponseStrategy serviceRequest( 
            final CommandExecutionParams params,
            final int httpResponseCode,
            final Object resultToProvideToClient )
    {
        String dataTag = DEFAULT_DATA_TAG;
        if ( null != resultToProvideToClient 
                && SimpleBeanSafeToProxy.class.isAssignableFrom( resultToProvideToClient.getClass() ) )
        {
            final CustomMarshaledTypeName customDataTag =
                    InterfaceProxyFactory.getType( resultToProvideToClient.getClass() ).getAnnotation( 
                            CustomMarshaledTypeName.class );
            if ( null != customDataTag )
            {
                dataTag = customDataTag.value();
                if ( dataTag.isEmpty() )
                {
                    return serviceRequest( params, httpResponseCode, resultToProvideToClient, null );
                }
            }
        }
        
        return serviceRequest( params, httpResponseCode, resultToProvideToClient, dataTag );
    }
    
    
    private static ServletResponseStrategy serviceRequest( 
            final CommandExecutionParams params,
            final int httpResponseCode,
            final Object resultToProvideToClient,
            final String dataTag )
    {
        final BeanServletParams beanServletParams = BeanFactory.newBean( BeanServletParams.class );
        beanServletParams.setHttpResponseCode( httpResponseCode );
        beanServletParams.setDataTag( dataTag );
        beanServletParams.setResponse( resultToProvideToClient );
        beanServletParams.setRequestPath( params.getRequest().getRequestPath() );
        beanServletParams.setContentType( params.getRequest().getHttpRequest().getContentType() );
        if ( null != params.getRequest().getHttpRequest().getHeader( S3HeaderType.ACCEPT ) )
        {
            beanServletParams.setContentType( 
                    params.getRequest().getHttpRequest().getHeader( S3HeaderType.ACCEPT ) );
        }
        final String ncHeader = params.getRequest().getHttpRequest().getHeader( 
                S3HeaderType.NAMING_CONVENTION );
        beanServletParams.setNamingConvention( ( null == ncHeader ) ? 
                BaseMarshalable.DEFAULT_NAMING_CONVENTION : NamingConventionType.from( ncHeader ) );
        save( params, beanServletParams );
        
        return SERVLET_SPEC;
    }
    
    
    public static void setResponseTypeListener( final BeanServletResponseListener listener )
    {
        s_responseListener = listener;
    }
    
    
    public interface BeanServletResponseListener
    {
        void responseReturned( final Object response );
    } // end inner class def
    
    
    private static volatile BeanServletResponseListener s_responseListener;
    private static final Logger LOG = Logger.getLogger( BeanServlet.class );
    private static final String DEFAULT_DATA_TAG = "data";
    private static final ServletResponseStrategy SERVLET_SPEC = 
            new ServletResponseStrategyImpl( BeanServlet.class );
}
