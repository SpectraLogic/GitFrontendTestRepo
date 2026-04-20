/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.server.handler.reqhandler.amazons3.GetBucketRequestHandler;
import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetRequestHandlersFormalApiContractRequestHandler_Test 
{
    @Test
    public void testGetContractReturnsContract()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.REQUEST_HANDLER_CONTRACT );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( GetBucketRequestHandler.class.getName() );
    }
}
