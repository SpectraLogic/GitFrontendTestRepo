/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system.CreateHeapDumpRequestHandler.Application;
import com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system.CreateHeapDumpRequestHandler.CreateHeapDumpParams;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class CreateHeapDumpRequestHandler_Test 
{
    @Test
    public void testCreateHeapDumpForDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DumpHeapInvocationHandler dumpHeap = new DumpHeapInvocationHandler();
        support.setPlannerInterfaceIh(
                MockInvocationHandler.forMethod(
                        DataPlannerResource.class.getMethod( "dumpHeap" ),
                        dumpHeap,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "_rest_/heap_dump" )
                        .addParameter(
                                CreateHeapDumpParams.APPLICATION,
                                Application.DATA_PLANNER.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        driver.assertResponseToClientXPathEquals( "/Data/Application", Application.DATA_PLANNER.toString() );
        driver.assertResponseToClientXPathEquals( "/Data/Path", "the_path_to_the_heap_dump" );
        
        assertEquals(
                1,
                dumpHeap.getDumpHeapInvocations(),
                "Shoulda called dump heap exactly once." );
    }
    
    
    private final class DumpHeapInvocationHandler implements InvocationHandler
    {
        @Override
        public Object invoke( final Object proxy, final Method method, final Object[] args )
                throws Throwable
        {
            ++m_dumpHeapInvocations;
            return new RpcResponse<>( "the_path_to_the_heap_dump" );
        }


        public int getDumpHeapInvocations()
        {
            return m_dumpHeapInvocations;
        }
        
        
        private int m_dumpHeapInvocations = 0;
    }//end inner class
}
