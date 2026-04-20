/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;

public final class MockInternalRequestAuthorizationStrategy implements MockAuthorizationStrategy
{
    public void initializeDriver( final MockHttpRequestDriver driver )
    {
        driver.addHeader( 
                S3HeaderType.INTERNAL_REQUEST_REQUIRING_AUTH_BYPASS,
                "1" );
        if ( null != m_user )
        {
            driver.addHeader( 
                    S3HeaderType.IMPERSONATE_USER, 
                    m_user );
        }
    }
    
    
    public MockInternalRequestAuthorizationStrategy impersonate( final String user )
    {
        m_user = user;
        return this;
    }
    
    
    private volatile String m_user;
}
