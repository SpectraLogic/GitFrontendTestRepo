/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class VerifySystemHealthRequestHandler_Test 
{
    @Test
    public void testVerifyWhenDataPlannerIsDeadResultsIn503()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( new InvocationHandler()
        {
            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
            {
                return Boolean.FALSE;
            }
        } );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SYSTEM_HEALTH );
        driver.run();
        driver.assertHttpResponseCodeEquals( 503 );
        
    }
    
    
    @Test
    public void testVerifyWhenDataPlannerIsServiceableResultsIn200()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( new InvocationHandler()
        {
            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
            {
                return Boolean.TRUE;
            }
        } );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.SYSTEM_HEALTH );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
