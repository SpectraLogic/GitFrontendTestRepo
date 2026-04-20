/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner;

import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetDataPathBackendRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
