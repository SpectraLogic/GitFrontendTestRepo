/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.rest;

import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;

public final class RestRequestImpl implements RestRequest
{
    private RestRequestImpl( final RestActionType restAction, final RestDomainType domain, final String id )
    {
        m_restAction = restAction;
        m_domain = domain;
        m_id = id;
        m_violation = null;
        
        Validations.verifyNotNull( "REST action", m_restAction );
        Validations.verifyNotNull( "Domain", m_domain );
        if ( null != m_id && !m_restAction.isIdApplicable() )
        {
            throw new IllegalArgumentException( "ID is not applicable for " + m_restAction + "." );
        }
        switch ( domain.getResourceType() )
        {
            case SINGLETON:
                if ( !isValidForSingletonDomain() )
                {
                    throw new IllegalArgumentException(
                            "REST request is malformed for singleton resource type." );
                }
                break;
            case NON_SINGLETON:
                if ( !isValidForNonSingletonDomain() )
                {
                    throw new IllegalArgumentException(
                            "REST request is malformed for non-singleton resource type." );
                }
                break;
            default:
                throw new UnsupportedOperationException( 
                        "No code to handle " + domain.getResourceType() + "." );
        }
    }
    
    
    private RestRequestImpl( final String violation )
    {
        m_restAction = null;
        m_domain = null;
        m_id = null;
        m_violation = violation;
    }
    
    
    public static RestRequest valueOf( final RequestType requestType, final String requestPath )
    {
        Validations.verifyNotNull( "Request type", requestType );
        Validations.verifyNotNull( "Request path", requestPath );
        try
        {
            return valueOfInternal( requestType, requestPath );
        }
        catch ( final RuntimeException ex )
        {
            LOG.info( ExceptionUtil.getMessageWithSingleLineStackTrace( 
                    "Failed generating REST request.", ex ) );
            return new RestRequestImpl( ex.getMessage() );
        }
    }
    
    
    private static RestRequest valueOfInternal( final RequestType requestType, final String requestPath )
    {
        final List< String > rawElements = CollectionFactory.toList( requestPath.split( "/" ) );
        rawElements.remove( "" );
        final String [] elements = CollectionFactory.toArray( String.class, rawElements );
        if ( 2 > elements.length )
        {
            return NON_REST_REQUEST;
        }
        if ( !elements[ 0 ].equalsIgnoreCase( S3Utils.REST_REQUEST_REQUIRED_PREFIX ) )
        {
            return NON_REST_REQUEST;
        }
        
        String domainPart = elements[ 1 ];
        if ( domainPart.contains( "." ) )
        {
            domainPart = domainPart.split( "\\." )[ 0 ];
        }
        domainPart = NamingConventionType.CONSTANT.convert( domainPart );
        
        final RestDomainType domain = DOMAIN_TYPE_CACHE.get( domainPart );
        if ( null == domain )
        {
            return new RestRequestImpl(
                    "Domain '" + domainPart + "' does not exist.  Supported domains: " 
                    + CollectionFactory.toList( RestDomainType.values() ) );
        }
        
        String id = null;
        if ( 2 < elements.length )
        {
            id = "";
            for ( int i = 2; i < elements.length; ++i )
            {
                if ( 2 < i )
                {
                    id += "/";
                }
                id += elements[ i ];
            }
        }
        switch ( requestType )
        {
            case DELETE:
                if ( null == id )
                {
                    return new RestRequestImpl( RestActionType.BULK_DELETE, domain, id );
                }
                return new RestRequestImpl( RestActionType.DELETE, domain, id );
            case POST:
                if ( null == id )
                {
                    return new RestRequestImpl( RestActionType.CREATE, domain, id );
                }
                return new RestRequestImpl( "A post request cannot include an id." );
            case PUT:
                if ( null == id )
                {
                    return new RestRequestImpl( RestActionType.BULK_MODIFY, domain, null );
                }
                return new RestRequestImpl( RestActionType.MODIFY, domain, id );
            case GET:
                if ( null == id )
                {
                    return new RestRequestImpl( RestActionType.LIST, domain, id );
                }
                if ( "new".equalsIgnoreCase( id ) )
                {
                    return new RestRequestImpl( RestActionType.CREATE, domain, null );
                }
                return new RestRequestImpl( RestActionType.SHOW, domain, id );
            case HEAD:
                return new RestRequestImpl( "HTTP Head method is invalid." );
            default:
                return new RestRequestImpl( "No code to support " + requestType + "." );
        }
    }
    
    
    public void validate()
    {
        if ( null != m_violation )
        {
            throw new S3RestException(
                    AWSFailure.INVALID_URI,
                    m_violation + "  To see all the supported REST requests, go to " 
                    + "http[s]://{datapath ip or DNS name of appliance}/"
                    + S3Utils.REST_REQUEST_REQUIRED_PREFIX
                    + "/request_handler in the browser of your choice." );
        }
    }
    
    
    public boolean isValidRestRequest()
    {
        return ( null != m_restAction );
    }
    
    
    public RestActionType getAction()
    {
        validateIsRestful();
        return m_restAction;
    }
    
    
    public RestDomainType getDomain()
    {
        validateIsRestful();
        return m_domain;
    }
    
    
    public < T extends SimpleBeanSafeToProxy > T getBean( final BeansRetriever< T > retriever )
    {
        validateIsRestful();
        Validations.verifyNotNull( "Retriever", retriever );
        if ( null == m_id )
        {
            return retriever.attain( Require.nothing() );
        }
        return retriever.discover( m_id );
    }
    
    
    public UUID getId( final BeansRetriever< ? extends Identifiable > retriever )
    {
        validateIsRestful();
        Validations.verifyNotNull( "Retriever", retriever );
        if ( null == m_id || m_id.isEmpty() )
        {
            throw new S3RestException( 
                    AWSFailure.INVALID_ARGUMENT, 
                    "A UUID id must be specified, but no ID was specified at all." );
        }
        try
        {
            return UUID.fromString( m_id );
        }
        catch ( final Exception ex )
        {
            Validations.verifyNotNull( "Shut up CodePro", ex );
            return getBean( retriever ).getId();
        }
    }
    
    
    public String getIdAsString()
    {
        return m_id;
    }
    
    
    public boolean isValidForSingletonDomain()
    {
        if ( !isValidRestRequest() )
        {
            return false;
        }
        
        return ( m_id == null );
    }
    
    
    public boolean isValidForNonSingletonDomain()
    {
        if ( !isValidRestRequest() )
        {
            return false;
        }
        
        return ( m_id != null || !m_restAction.isIdApplicable() );
    }
    
    
    private void validateIsRestful()
    {
        if ( !isValidRestRequest() )
        {
            throw new IllegalStateException( "Request is not RESTful." );
        }
    }
    
    
    private final static class DomainTypeCacheResultProvider
        implements CacheResultProvider< String, RestDomainType >
    {
        public RestDomainType generateCacheResultFor( final String domain )
        {
            for ( final RestDomainType type : RestDomainType.values() )
            {
                if ( type.toString().equals( domain ) )
                {
                    return type;
                }
            }

            final StringBuilder msg = new StringBuilder();
            msg.append( "Domain '" + domain + "' does not match any known domains:" );
            msg.append( Platform.NEWLINE );
            for ( final RestDomainType type : RestDomainType.values() )
            {
                msg.append( "  " ).append( type.toString() ).append( " / " )
                   .append( type.toString() ).append( Platform.NEWLINE );
            }
            LOG.info( msg );
            return null;
        }
    } // end inner class def
    
    
    private final RestActionType m_restAction;
    private final RestDomainType m_domain;
    private final String m_id;
    private final String m_violation;
    
    private final static RestRequest NON_REST_REQUEST = new RestRequestImpl( null );
    private final static Logger LOG = Logger.getLogger( RestRequestImpl.class );
    private final static StaticCache< String, RestDomainType > DOMAIN_TYPE_CACHE =
            new StaticCache<>( new DomainTypeCacheResultProvider() );
}
