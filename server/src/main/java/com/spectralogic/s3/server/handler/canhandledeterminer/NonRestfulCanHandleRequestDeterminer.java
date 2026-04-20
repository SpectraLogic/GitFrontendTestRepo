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
import java.util.Set;

import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

/**
 * <font color = red>
 * Only use this determiner if you have to in order to be compliant with Amazon's S3 API.
 * Always favor RESTful-formatted request handling otherwise.
 * </font>
 */
public final class NonRestfulCanHandleRequestDeterminer implements CanHandleRequestDeterminer
{
    /**
     * <font color = red>
     * Only use this determiner if you have to in order to be compliant with Amazon's S3 API.
     * Always favor RESTful-formatted request handling otherwise.
     * </font>
     */
    public NonRestfulCanHandleRequestDeterminer(
            final RequestType requestTypeRequired,
            final BucketRequirement bucketRequirement,
            final S3ObjectRequirement objectRequirement )
    {
        m_bucketRequirement = bucketRequirement;
        m_objectRequirement = objectRequirement;
        m_requestRequirements = CollectionFactory.toSet( 
                ( null == requestTypeRequired ) ? null : new RequestTypeRequirement( requestTypeRequired ),
                m_bucketRequirement,
                m_objectRequirement,
                m_queryStringRequirement );
        m_requestRequirements.remove( null );
        m_requiredRequestType = requestTypeRequired;
    }
    
    
    public String getFailureToHandle( final DS3Request request )
    {
        Validations.verifyNotNull( "Request", request );
        
        if ( request.getRestRequest().isValidRestRequest() )
        {
            return "";
        }
        
        for ( final Requirement r : m_requestRequirements )
        {
            if ( !r.meetsRequirement( request ) )
            {
                if ( m_queryStringRequirement == r )
                {
                    return r.getRequirementDescription();
                }
                return "";
            }
        }
        
        return null;
    }
    
    
    public List< String > getHandlingRequirements()
    {
        final List< String > retval = new ArrayList<>();
        retval.add( "Must be an AWS-style request" );
        for ( final Requirement r : m_requestRequirements )
        {
            final String requirementDescription = r.getRequirementDescription();
            if ( null != requirementDescription )
            {
                retval.add( requirementDescription );
            }
        }
        Collections.sort( retval );
        
        return retval;
    }
    
    
    public String getSampleRequest()
    {
        return CanHandleRequestDeterminerUtils.sanitizeSampleUrl( 
                CanHandleRequestDeterminer.SAMPLE_REQUEST_PREFIX
                + m_bucketRequirement.getSampleText()
                + m_objectRequirement.getSampleText()
                + m_queryStringRequirement.getSampleQueryString(
                        CollectionFactory.< RequestParameterType >toSet(),
                        CollectionFactory.< String >toSet() ) );
    }
    
    
    public RequestHandlerRequestContract getRequestContract()
    {
        final RequestHandlerRequestContract retval = 
                BeanFactory.newBean( RequestHandlerRequestContract.class );
        retval.setRequiredParams( m_queryStringRequirement.getParamsContract( true ) );
        retval.setOptionalParams( m_queryStringRequirement.getParamsContract( false ) );
        retval.setHttpVerb( m_requiredRequestType );
        retval.setBucketRequirement( m_bucketRequirement );
        retval.setObjectRequirement( m_objectRequirement );
        
        return retval;
    }
    
    
    public QueryStringRequirement getQueryStringRequirement()
    {
        return m_queryStringRequirement;
    }
    

    private final BucketRequirement m_bucketRequirement;
    private final S3ObjectRequirement m_objectRequirement;
    private final QueryStringRequirementImpl m_queryStringRequirement =
            new QueryStringRequirementImpl();
    private final Set< Requirement > m_requestRequirements;
    private final RequestType m_requiredRequestType;
}
