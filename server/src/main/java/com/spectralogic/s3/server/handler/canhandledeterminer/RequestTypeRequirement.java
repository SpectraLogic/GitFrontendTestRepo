/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.Validations;

final class RequestTypeRequirement implements Requirement
{
    RequestTypeRequirement( final RequestType requestType )
    {
        m_requestType = requestType;
        Validations.verifyNotNull( "Request type", m_requestType );
    }
    
    
    public boolean meetsRequirement( final DS3Request request )
    {
        return ( request.getHttpRequest().getType() == m_requestType );
    }
    
    
    public String getRequirementDescription()
    {
        return "Must be HTTP request type " + m_requestType;
    }
    
    
    private final RequestType m_requestType;
}
