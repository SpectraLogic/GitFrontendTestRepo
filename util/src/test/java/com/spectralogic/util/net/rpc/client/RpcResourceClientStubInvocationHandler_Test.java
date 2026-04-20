/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.mockresource.UserResource;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class RpcResourceClientStubInvocationHandler_Test 
{

    @Test
    public void testConstructorNullResourceTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new RpcResourceClientStubInvocationHandler( 
                        null,
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, null ),
                        50,
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
            }
        } );
    }
    

    @Test
    public void testConstructorNullRpcClientNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new RpcResourceClientStubInvocationHandler( 
                        "resourceType",
                        null,
                        null,
                        50, 
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
            }
        } );
    }
    

    @Test
    public void testConstructorNullConcurrentRequestExecutionPolicyNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new RpcResourceClientStubInvocationHandler( 
                        "resourceType",
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, null ),
                        50,
                        null );
            }
        } );
    }
    

    @Test
    public void testEqualsReturnsAsExpected()
    {
        final BasicTestsInvocationHandler rpcClientBtih = new BasicTestsInvocationHandler( null );
        final RpcResourceClientStubInvocationHandler ih = 
                new RpcResourceClientStubInvocationHandler( 
                        "resourceType",
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, rpcClientBtih ),
                        50, 
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
        final UserResource userResource = InterfaceProxyFactory.getProxy( UserResource.class, ih );
        
        assertTrue(
                userResource.equals( userResource ),
                "Shoulda said was equal to self."
                 );
        assertFalse(
                userResource.equals( this ),
                "Shoulda said was not equal to other."
                 );
        assertEquals(0,
                rpcClientBtih.getTotalCallCount(),
                "Should notta made any calls on the rpc client.");
    }
    

    @Test
    public void testHashCodeReturnsAsExpected()
    {
        final BasicTestsInvocationHandler rpcClientBtih = new BasicTestsInvocationHandler( null );
        final RpcResourceClientStubInvocationHandler ih = 
                new RpcResourceClientStubInvocationHandler( 
                        "resourceType",
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, rpcClientBtih ),
                        50, 
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
        final UserResource userResource = InterfaceProxyFactory.getProxy( UserResource.class, ih );
        
        assertTrue(
                userResource.hashCode() == userResource.hashCode(),
                "Shoulda said was equal to self."
                 );
        assertFalse(
                userResource.hashCode() == this.hashCode(),
                "Shoulda said was not equal to other."
                 );
        assertEquals(0, rpcClientBtih.getTotalCallCount(), "Should notta made any calls on the rpc client.");
    }
    

    @Test
    public void testToStringDoesNotBlowUp()
    {
        final BasicTestsInvocationHandler rpcClientBtih = new BasicTestsInvocationHandler( null );
        final RpcResourceClientStubInvocationHandler ih = 
                new RpcResourceClientStubInvocationHandler( 
                        "resourceType",
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, rpcClientBtih ),
                        50, 
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
        final UserResource userResource = InterfaceProxyFactory.getProxy( UserResource.class, ih );
        
        assertNotNull( "Shoulda returned something.", userResource.toString() );
    }
    

    @Test
    public void testGetConcurrentRequestExecutionPolicyReturnsAsExpected()
    {
        final BasicTestsInvocationHandler rpcClientBtih = new BasicTestsInvocationHandler( null );
        final RpcResourceClientStubInvocationHandler ih = 
                new RpcResourceClientStubInvocationHandler( 
                        "resourceType",
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, rpcClientBtih ),
                        50, 
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
        final UserResource userResource = InterfaceProxyFactory.getProxy( UserResource.class, ih );

        assertEquals(ConcurrentRequestExecutionPolicy.CONCURRENT, userResource.getConcurrentRequestExecutionPolicy(), "Should notta made any calls on the rpc client.");
    }
    
    
    @Test
    public void testInvokeRpcMethodResultsInInvocationOnRpcClientManager()
    {
        final BasicTestsInvocationHandler rpcClientBtih = new BasicTestsInvocationHandler( null );
        final RpcResourceClientStubInvocationHandler ih = 
                new RpcResourceClientStubInvocationHandler( 
                        "my_type",
                        "someInstanceName",
                        InterfaceProxyFactory.getProxy( RpcClient.class, rpcClientBtih ),
                        10,
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
        final UserResource userResource = InterfaceProxyFactory.getProxy( UserResource.class, ih );
        TestUtil.assertThrows( null, RpcRequestUnserviceableException.class, new BlastContainer()
        {

            public void test() throws Throwable
            {
                userResource.createUser( "name", "emailAddress" );
            }
        } );
    }
    
    
    @Test
    public void testCallInvalidMethodNotSupported()
    {
        final BasicTestsInvocationHandler rpcClientBtih = new BasicTestsInvocationHandler( null );
        final RpcResourceClientStubInvocationHandler ih = 
                new RpcResourceClientStubInvocationHandler( 
                        "my_type",
                        null,
                        InterfaceProxyFactory.getProxy( RpcClient.class, rpcClientBtih ),
                        100,
                        ConcurrentRequestExecutionPolicy.CONCURRENT );
        final UserResource userResource = InterfaceProxyFactory.getProxy( UserResource.class, ih );
        
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test()
                    {
                        userResource.thisIsAnInvalidRpcMethodSinceItDoesNotReturnAnRpcFuture();
                    }
        } );
    }
}
