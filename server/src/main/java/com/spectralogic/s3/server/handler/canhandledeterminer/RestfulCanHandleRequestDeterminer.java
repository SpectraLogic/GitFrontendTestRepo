/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.request.rest.RestResourceType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

/**
 * Prefer RESTful request handling whenever possible (use this request determiner whenever possible).
 */
public final class RestfulCanHandleRequestDeterminer implements CanHandleRequestDeterminer
{
    public RestfulCanHandleRequestDeterminer(
            final RestActionType restAction,
            final RestDomainType restDomain )
    {
        this( restAction, null, restDomain );
    }
    
    
    public RestfulCanHandleRequestDeterminer(
            final RestOperationType operation,
            final RestDomainType restDomain )
    {
        this( RestActionType.MODIFY, operation, restDomain );
    }
    
    
    public RestfulCanHandleRequestDeterminer(
            final RestActionType restAction,
            final RestOperationType operation,
            final RestDomainType restDomain )
    {
        m_restAction = restAction;
        m_restDomain = restDomain;
        m_operation = operation;
        Validations.verifyNotNull( "Action", m_restAction );
        Validations.verifyNotNull( "Domain", m_restDomain );
        if ( null != m_operation )
        {
            m_queryStringRequirement.registerRequiredRequestParameters( RequestParameterType.OPERATION );
        }
    }
    
    
    public String getFailureToHandle( final DS3Request request )
    {
        if ( !request.getRestRequest().isValidRestRequest() )
        {
            return "";
        }
        if ( request.getRestRequest().getDomain() != m_restDomain )
        {
            return "";
        }
        
        final FailuresToHandle retval = new FailuresToHandle();
        final RestOperationType actualOperation = 
                ( null == request.getRequestParameter( RequestParameterType.OPERATION ) ) ?
                        null
                        : request.getRequestParameter( RequestParameterType.OPERATION ).getEnum(
                                RestOperationType.class );
        if ( request.getRestRequest().getAction() != m_restAction )
        {
            retval.add( "RESTful action required: " + m_restAction );
        }
        if ( m_operation != actualOperation )
        {
            retval.add(
                    "Required " + RequestParameterType.OPERATION + " query parameter value: " + m_operation );
        }
        for ( final String failure : m_queryStringRequirement.getFailures( request ) )
        {
            retval.addQueryParamViolation( failure );
        }
        return retval.getRetval();
    }
    
    
    private final static class FailuresToHandle
    {
        private void add( final String failure )
        {
            if ( 0 == failure.length() )
            {
                throw new IllegalArgumentException( "Failure cannot be of zero length." );
            }
            if ( 0 != m_failures.length() )
            {
                m_failures.append( ".  " );
            }
            m_failures.append( failure );
            ++m_failureCount;
        }
        
        private void addQueryParamViolation( final String failure )
        {
            add( failure );
            if ( m_addedQueryParamViolation )
            {
                --m_failureCount;
            }
            m_addedQueryParamViolation = true;
        }
        
        private String getRetval()
        {
            if ( 0 == m_failures.length() )
            {
                return null;
            }
            if ( 1 < m_failureCount )
            {
                return "";
            }
            return m_failures.toString();
        }

        private int m_failureCount;
        private boolean m_addedQueryParamViolation;
        private final StringBuilder m_failures = new StringBuilder();
    } // end inner class def
    
    
    public List< String > getHandlingRequirements()
    {
        final List< String > retval = new ArrayList<>();
        retval.add( "Must be a DS3-style request" );
        retval.add( "Must be REST action " + m_restAction );
        retval.add( "Must be REST domain " + m_restDomain );
        final String requirementDescription = m_queryStringRequirement.getRequirementDescription();
        if ( null != requirementDescription )
        {
            retval.add( requirementDescription );
        }
        Collections.sort( retval );
        
        return retval;
    }
    
    
    public String getSampleRequest()
    {
        final StringBuilder stringBuilder = new StringBuilder()
                .append( CanHandleRequestDeterminer.SAMPLE_REQUEST_PREFIX )
                .append( S3Utils.REST_REQUEST_REQUIRED_PREFIX )
                .append( "/" )
                .append( m_restDomain.toString() )
                .append( "/" );
        if ( m_restAction.isIdApplicable() 
                && m_restDomain.getResourceType() == RestResourceType.NON_SINGLETON )
        {
            stringBuilder.append( "{unique identifier or attribute}/" );
        }
        if ( null != m_operation )
        {
            stringBuilder
                    .append( "?" )
                    .append( RequestParameterType.OPERATION.toString() )
                    .append( "=" )
                    .append( m_operation.toString() );
        }
        return CanHandleRequestDeterminerUtils.sanitizeSampleUrl( stringBuilder
                .append( m_queryStringRequirement.getSampleQueryString(
                        CollectionFactory.toSet( RequestParameterType.OPERATION ),
                        CollectionFactory.< String >toSet() ) )
                .toString() );
    }
    
    
    public RequestHandlerRequestContract getRequestContract()
    {
        final RequestHandlerRequestContract retval = 
                BeanFactory.newBean( RequestHandlerRequestContract.class );
        retval.setAction( m_restAction );
        retval.setIncludeIdInPath(
                m_restAction.isIdApplicable()
                && RestResourceType.NON_SINGLETON == m_restDomain.getResourceType() );
        retval.setHttpVerb( m_restAction.getDefaultHttpVerb() );
        retval.setResource( m_restDomain );
        retval.setResourceType( m_restDomain.getResourceType() );
        retval.setOperation( m_operation );
        retval.setRequiredParams( m_queryStringRequirement.getParamsContract( true ) );
        retval.setOptionalParams( m_queryStringRequirement.getParamsContract( false ) );
        
        return retval;
    }
    
    
    public QueryStringRequirement getQueryStringRequirement()
    {
        return m_queryStringRequirement;
    }
    
    
    private final RestActionType m_restAction;
    private final RestDomainType m_restDomain;
    private final RestOperationType m_operation;
    private final QueryStringRequirementImpl m_queryStringRequirement =
            new QueryStringRequirementImpl();
}
