/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetSystemInformationRequestHandler_Test 
{
    @Test
    public void testGetSystemInformationRequestHandlerDoesNotBlowUp()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SYSTEM_INFORMATION );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
