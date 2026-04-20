/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import java.util.List;

import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.net.rpc.frmwrk.QuiescableRpcResource;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class QuiesceDataPathToPrepareForShutdownRequestHandler_Test 
{
    @Test
    public void testQuiesceWithoutAnyOptionalParamsDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.DATA_PATH.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        List< MethodInvokeData > invocations = support.getTargetInterfaceBtih().getMethodInvokeData(
                ReflectUtil.getMethod( QuiescableRpcResource.class, "quiesceAndPrepareForShutdown" ) );
        assertEquals(1,  invocations.size(), "Shoulda called quiesce exactly once with correct defaults.");
        assertEquals(Boolean.FALSE, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda called quiesce exactly once with correct defaults.");

        invocations = support.getTapeInterfaceBtih().getMethodInvokeData(
                ReflectUtil.getMethod( QuiescableRpcResource.class, "quiesceAndPrepareForShutdown" ) );
        assertEquals(1,  invocations.size(), "Shoulda called quiesce exactly once with correct defaults.");
        assertEquals(Boolean.FALSE, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda called quiesce exactly once with correct defaults.");
    }
    
    
    @Test
    public void testQuiesceWithAllOptionalParamsDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.DATA_PATH.toString() )
                    .addParameter( RequestParameterType.FORCE.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        List< MethodInvokeData > invocations = support.getTargetInterfaceBtih().getMethodInvokeData(
                ReflectUtil.getMethod( QuiescableRpcResource.class, "quiesceAndPrepareForShutdown" ) );
        assertEquals(1,  invocations.size(), "Shoulda called quiesce exactly once with correct defaults.");
        assertEquals(Boolean.TRUE, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda called quiesce exactly once with correct defaults.");

        invocations = support.getTapeInterfaceBtih().getMethodInvokeData(
                ReflectUtil.getMethod( QuiescableRpcResource.class, "quiesceAndPrepareForShutdown" ) );
        assertEquals(1,  invocations.size(), "Shoulda called quiesce exactly once with correct defaults.");
        assertEquals(Boolean.TRUE, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda called quiesce exactly once with correct defaults.");
    }
}
