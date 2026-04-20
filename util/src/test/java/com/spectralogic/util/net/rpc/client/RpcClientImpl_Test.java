/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.mockresource.UserResource;
import com.spectralogic.util.net.tcpip.TcpIpServerTestPort;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class RpcClientImpl_Test 
{
    @Test
    public void testConstructorNullHostNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new RpcClientImpl( null, TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
                }
            } );
    }
    
    
    @Test
    public void testGetRpcResourceNullApiNotAllowed()
    {
        final RpcClient client =
                new RpcClientImpl( "localhost", TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    client.getRpcResource( null, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
                }
            } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    client.getRpcResource( null, null, ConcurrentRequestExecutionPolicy.CONCURRENT );
                }
            } );
    }
    
    
    @Test
    public void testAddRpcCompletedListenerNullListenerNotAllowed()
    {
        final RpcClient client =
                new RpcClientImpl( "client", TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    client.addRpcCompletedListener( null );
                }
            } );
    }
    
    
    @Test
    public void testCannotMakeAnyCallsOnceShutdown()
    {
        final RpcClient client =
                new RpcClientImpl( "client", TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        client.shutdown();
        @SuppressWarnings( "unchecked" )
        final RpcCompletedListener< Object > listener = 
                InterfaceProxyFactory.getProxy( RpcCompletedListener.class, null );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    client.addRpcCompletedListener( listener );
                }
            } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                client.getRpcResource(
                        UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                client.invokeRemoteProcedureCall( 
                        "blah", null, "blah", String.class, RpcMethodNullReturn.OPTIONAL, new ArrayList<>() );
            }
        } );
    }
}
