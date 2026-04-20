/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import com.spectralogic.s3.server.request.api.DS3Request;

public enum S3ObjectRequirement implements Requirement
{
    REQUIRED( Boolean.TRUE ),
    OPTIONAL( null ),
    NOT_ALLOWED( Boolean.FALSE );
    
    
    private S3ObjectRequirement( final Boolean shouldHaveObject )
    {
        m_shouldHaveObject = shouldHaveObject;
    }


    public boolean meetsRequirement( final DS3Request request )
    {
        if ( null == m_shouldHaveObject )
        {
            return true;
        }
        
        final boolean hasObject = ( null != request.getObjectName() );
        return ( hasObject == m_shouldHaveObject.booleanValue() );
    }
    
    
    public String getRequirementDescription()
    {
        if ( null == m_shouldHaveObject )
        {
            return null;
        }
        if ( m_shouldHaveObject.booleanValue() )
        {
            return "Must include an S3 object specification";
        }
        return "Cannot include an S3 object specification";
    }


    String getSampleText()
    {
        if ( null == m_shouldHaveObject )
        {
            return "{object, if applicable}";
        }
        if ( m_shouldHaveObject.booleanValue() )
        {
            return "{object}";
        }
        return "";
    }
    
    
    private final Boolean m_shouldHaveObject;
}
