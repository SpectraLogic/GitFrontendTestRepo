/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import com.spectralogic.s3.server.request.api.DS3Request;

public enum BucketRequirement implements Requirement
{
    REQUIRED( Boolean.TRUE ),
    OPTIONAL( null ),
    NOT_ALLOWED( Boolean.FALSE );
    
    
    private BucketRequirement( final Boolean shouldHaveBucket )
    {
        m_shouldHaveBucket = shouldHaveBucket;
    }


    public boolean meetsRequirement( final DS3Request request )
    {
        if ( null == m_shouldHaveBucket )
        {
            return true;
        }
        
        final boolean hasBucket = ( null != request.getBucketName() );
        return ( hasBucket == m_shouldHaveBucket.booleanValue() );
    }
    
    
    public String getRequirementDescription()
    {
        if ( null == m_shouldHaveBucket )
        {
            return null;
        }
        if ( m_shouldHaveBucket.booleanValue() )
        {
            return "Must include an S3 bucket specification";
        }
        return "Cannot include an S3 bucket specification";
    }
    
    
    String getSampleText()
    {
        if ( null == m_shouldHaveBucket )
        {
            return "{bucket, if applicable}/";
        }
        if ( m_shouldHaveBucket.booleanValue() )
        {
            return "{bucket}/";
        }
        return "";
    }
    
    
    private final Boolean m_shouldHaveBucket;
}
