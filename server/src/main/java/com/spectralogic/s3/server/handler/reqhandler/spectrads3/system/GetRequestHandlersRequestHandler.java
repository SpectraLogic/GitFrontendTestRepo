/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.CanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.find.RequestHandlerProvider;
import com.spectralogic.s3.server.handler.reqhandler.ExcludeRequestHandlerResponseDocumentation;
import com.spectralogic.s3.server.handler.reqhandler.RequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanComparator.BeanPropertyComparisonSpecifiction;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;
import com.spectralogic.util.security.ChecksumGenerator;

@ExcludeRequestHandlerResponseDocumentation
public final class GetRequestHandlersRequestHandler extends BaseRequestHandler
{
    public GetRequestHandlersRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.NONE ), 
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST,
                       RestDomainType.REQUEST_HANDLER ) );
        
        registerOptionalRequestParameters( RequestParameterType.FULL_DETAILS );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        if ( params.getRequest().hasRequestParameter( RequestParameterType.FULL_DETAILS ) )
        {
            return BeanServlet.serviceGet( params, getAllRequestHandlers() );
        }
        
        getAllRequestHandlers();
        return BeanServlet.serviceGet( params, ALL_REQUEST_HANDLERS_WITH_SHORT_DETAILS );
    }
    
    
    public static Map< Class< ? >, String > getRequestHandlerVersions()
    {
        final Map< Class< ? >, String > retval = new HashMap<>();
        for ( final RequestHandlerInfo rh : getAllRequestHandlers().getRequestHandlers() )
        {
            try
            {
                retval.put( Class.forName( rh.getName() ), rh.getVersion() );
            }
            catch ( final ClassNotFoundException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        return retval;
    }
    
    
    static AllRequestHandlers getAllRequestHandlers()
    {
        synchronized ( LOCK )
        {
            if ( null != ALL_REQUEST_HANDLERS.getRequestHandlers() )
            {
                return ALL_REQUEST_HANDLERS;
            }
            
            final InputStream propertiesIs = 
                    GetRequestHandlersRequestHandler.class.getResourceAsStream( "/requesthandlers.props" );
            final InputStream srPropertiesIs = 
                    GetRequestHandlersRequestHandler.class.getResourceAsStream(
                            "/requesthandlerresponses.props" );
            final Properties properties = new Properties();
            final Properties srProperties = new Properties();
            try
            {
                properties.load( propertiesIs );
                propertiesIs.close();
                srProperties.load( srPropertiesIs );
                srPropertiesIs.close();
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to load request handler documentation.", ex );
            }
            
            final Map< String, List< RequestHandlerExampleResponse > > requestHandlerResponses = 
                    new HashMap<>();
            for ( final Map.Entry< Object, Object > e : srProperties.entrySet() )
            {
                final String fullKey = e.getKey().toString();
                if ( !fullKey.endsWith( "." + RequestHandlerExampleResponse.HTTP_RESPONSE_CODE ) )
                {
                    continue;
                }
                final String keyPrefix = fullKey.substring( 0, fullKey.lastIndexOf( '.' ) );
                final String keyClass = keyPrefix.substring( 0, keyPrefix.lastIndexOf( '.' ) );
                if ( !requestHandlerResponses.containsKey( keyClass ) )
                {
                    requestHandlerResponses.put( keyClass, new ArrayList< RequestHandlerExampleResponse >() );
                }
                requestHandlerResponses.get( keyClass ).add( new RequestHandlerExampleResponse(
                        getProperty(
                                srProperties,
                                keyPrefix + "." + RequestHandlerExampleResponse.TEST ),
                        getProperty( 
                                srProperties,
                                keyPrefix + "." + RequestHandlerExampleResponse.HTTP_REQUEST ),
                        Integer.valueOf( e.getValue().toString() ).intValue(), 
                        getProperty(
                                srProperties, 
                                keyPrefix + "." + RequestHandlerExampleResponse.HTTP_RESPONSE ),
                        getProperty(
                                srProperties, 
                                keyPrefix + "." + RequestHandlerExampleResponse.HTTP_RESPONSE_TYPE ) ) );
            }
            
            final List< RequestHandlerInfo > shortResults = new ArrayList<>();
            final List< RequestHandlerInfo > results = new ArrayList<>();
            for ( final com.spectralogic.s3.server.handler.reqhandler.RequestHandler rh 
                    : RequestHandlerProvider.getAllRequestHandlers() )
            {
                final List< RequestHandlerExampleResponse > exampleResponses = 
                        requestHandlerResponses.get( rh.getClass().getName() );
                if ( null != exampleResponses )
                {
                    Collections.sort( 
                            exampleResponses,
                            new BeanComparator<>(
                                    RequestHandlerExampleResponse.class, 
                                    new BeanPropertyComparisonSpecifiction( 
                                            RequestHandlerExampleResponse.TEST,
                                            Direction.ASCENDING,
                                            null ),
                                    new BeanPropertyComparisonSpecifiction( 
                                            RequestHandlerExampleResponse.HTTP_REQUEST,
                                            Direction.ASCENDING,
                                            null ) ) );
                }
                shortResults.add( new RequestHandlerInfo(
                        rh,
                        properties.getProperty( rh.getClass().getName() + ".documentation" ),
                        properties.getProperty( rh.getClass().getName() + ".version" ),
                        new ArrayList< RequestHandlerExampleResponse >() ) );
                results.add( new RequestHandlerInfo(
                        rh,
                        properties.getProperty( rh.getClass().getName() + ".documentation" ),
                        properties.getProperty( rh.getClass().getName() + ".version" ),
                        exampleResponses ) );
            }
            Collections.sort( 
                    shortResults, 
                    new BeanComparator<>( RequestHandlerInfo.class, RequestHandlerInfo.NAME ) );
            Collections.sort( 
                    results,
                    new BeanComparator<>( RequestHandlerInfo.class, RequestHandlerInfo.NAME ) );
            
            ALL_REQUEST_HANDLERS_WITH_SHORT_DETAILS.setRequestHandlers( 
                    CollectionFactory.toArray( RequestHandlerInfo.class, shortResults ) );
            ALL_REQUEST_HANDLERS.setRequestHandlers(
                    CollectionFactory.toArray( RequestHandlerInfo.class, results ) );
            return ALL_REQUEST_HANDLERS;
        }
    }
    
    
    private static String getProperty( final Properties properties, final String key )
    {
        final Object retval = properties.get( key );
        if ( null == retval )
        {
            return "ERROR - key does not exist: " + key;
        }
        return retval.toString();
    }
    
    
    interface AllRequestHandlers extends SimpleBeanSafeToProxy
    {
        String REQUEST_HANDLERS = "requestHandlers";
        
        @CustomMarshaledName( value = "RequestHandler" )
        RequestHandlerInfo [] getRequestHandlers();
        
        void setRequestHandlers( final RequestHandlerInfo [] value );
    } // end inner class def
    
    
    final static class RequestHandlerInfo
        extends BaseMarshalable 
        implements Comparable< RequestHandlerInfo >
    {
        private RequestHandlerInfo(
                final RequestHandler requestHandler, 
                final String documentation,
                final String version,
                List< RequestHandlerExampleResponse > exampleResponses )
        {
            if ( null != requestHandler.getClass().getAnnotation( 
                    ExcludeRequestHandlerResponseDocumentation.class ) )
            {
                exampleResponses = new ArrayList<>();
            }
            
            final CanHandleRequestDeterminer requestDeterminer =
                    requestHandler.getCanHandleRequestDeterminer();
            m_handlerName = requestHandler.getClass().getName();
            m_requestRequirements = requestDeterminer.getHandlingRequirements();
            m_exampleResponses = exampleResponses;
            m_sampleUrl = requestDeterminer.getSampleRequest().toLowerCase();
            m_documentation = documentation;
            m_version = version + "." + ChecksumGenerator.generateMd5( 
                    requestDeterminer.getRequestContract().toJson( NamingConventionType.UNDERSCORED ) )
                    .toUpperCase();
        }
        
        private final static String NAME = "name";
        
        @MarshalXmlAsAttribute
        public String getName()
        {
            return m_handlerName;
        }
        
        public List< String > getRequestRequirements()
        {
            return m_requestRequirements;
        }
        
        public List< RequestHandlerExampleResponse > getSampleResponses()
        {
            return m_exampleResponses;
        }
        
        public String getSampleUrl()
        {
            return m_sampleUrl;
        }
        
        public String getDocumentation()
        {
            return m_documentation;
        }
        
        public String getVersion()
        {
            return m_version;
        }
        
        public int compareTo( final RequestHandlerInfo o )
        {
            return getName().compareTo( o.getName() );
        }
        
        private final String m_handlerName;
        private final String m_sampleUrl;
        private final List< String > m_requestRequirements;
        private final List< RequestHandlerExampleResponse > m_exampleResponses;
        private final String m_documentation;
        private final String m_version;
    } // end inner class def
    

    private final static AllRequestHandlers ALL_REQUEST_HANDLERS_WITH_SHORT_DETAILS =
            BeanFactory.newBean( AllRequestHandlers.class );
    private final static AllRequestHandlers ALL_REQUEST_HANDLERS =
            BeanFactory.newBean( AllRequestHandlers.class );
    private final static Object LOCK = new Object();
}
