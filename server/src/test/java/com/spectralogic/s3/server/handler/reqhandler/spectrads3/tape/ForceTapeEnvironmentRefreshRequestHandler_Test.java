/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;

public final class ForceTapeEnvironmentRefreshRequestHandler_Test 
{
    @Test
    public void testtestForceTapeEnvironmentRefreshDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final Method method = ReflectUtil.getMethod(
                TapeManagementResource.class, "forceTapeEnvironmentRefresh" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        support.setTapeInterfaceIh( btih );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.TAPE_ENVIRONMENT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                1,
                btih.getMethodCallCount( method ),
                "Shoulda sent the refresh request."
                );
    }
}
