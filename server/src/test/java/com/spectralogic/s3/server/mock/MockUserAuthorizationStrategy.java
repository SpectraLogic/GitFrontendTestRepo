/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;

public final class MockUserAuthorizationStrategy implements MockAuthorizationStrategy
{
    public MockUserAuthorizationStrategy( final String userId )
    {
        m_userId = userId;
    }

    
    public void initializeDriver( final MockHttpRequestDriver driver )
    {
        driver.addHeader( 
                S3HeaderType.IMPERSONATE_USER,
                m_userId );
    }
    
    
    private final String m_userId;
}
