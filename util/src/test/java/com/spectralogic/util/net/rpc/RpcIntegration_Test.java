/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc;

import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.client.*;
import com.spectralogic.util.net.rpc.domain.Request;
import com.spectralogic.util.net.rpc.domain.Response;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.mockresource.*;

import com.spectralogic.util.net.tcpip.TcpIpClient;
import com.spectralogic.util.net.tcpip.TcpIpClientImpl;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessage;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageEncoder;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.spectralogic.util.net.rpc.server.SerialAccessRpcResourceFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.net.rpc.server.RpcServerImpl;
import com.spectralogic.util.net.tcpip.TcpIpServerTestPort;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import com.spectralogic.util.net.rpc.frmwrk.RpcLogger;
import com.spectralogic.util.bean.BeanFactory;
import static org.junit.jupiter.api.Assertions.*;

//TODO: These tests are failing on the build server, but work locally.
// For now they are disabled in build server builds using SKIP_RPC_TESTS env variable.
@Tag("rpc-integration")
public class RpcIntegration_Test
{
   @Test
    public void testBasicCommunicationWorksInSendingRequestAndReceivingResponse()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource = new UserResourceImpl();
        final UserResource clientResource = client.getRpcResource(
                UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        server.register( null, resource );

        assertTrue(
                clientResource.isServiceable(),
                "Should be serviceable." );

        clientResource.createUser( "barry", "emailbarry" ).get( Timeout.DEFAULT );
        assertEquals(
                Boolean.FALSE,
                clientResource.exists( "jason" ).get( Timeout.DEFAULT ),
                "Should receive correct response  for client resource jason." );

        assertEquals(
                Boolean.TRUE,
                clientResource.exists( "barry" ).get( Timeout.DEFAULT ),
                "Should receive correct response for client resource barry.");
        
        final Duration duration = new Duration();
        int count = 0;
        while ( 333 > duration.getElapsedMillis() )
        {
            ++count;
            clientResource.exists( "barry" ).get( Timeout.DEFAULT );
        }
        LOG.info( "RPC calls are being serviced at the serial rate of " + ( count * 3 ) + " per second." );
        server.shutdown();
    }


   @Test
    public void testBasicCommunicationWorksWhenInstanceNamesAreUsed()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource1 = new UserResourceImpl();
        final UserResource clientResource1 = client.getRpcResource( 
                UserResource.class, "resource1", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        final UserResource resource2 = new UserResourceImpl();
        final UserResource clientResource2 = client.getRpcResource(
                UserResource.class, "resource2", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( "resource1", resource1 );
        server.register( "resource2", resource2 );
        
        assertTrue(
                clientResource1.isServiceable(),
                "Should be serviceable.");
        clientResource1.createUser( "barry", "emailbarry" ).get( Timeout.DEFAULT );
        assertEquals(
                Boolean.FALSE,
                clientResource1.exists( "jason" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        assertEquals(
                Boolean.TRUE,
                clientResource1.exists( "barry" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        
        assertTrue(
                clientResource2.isServiceable(),
                "Should be serviceable.");
        clientResource2.createUser( "jason", "emailjason" ).get( Timeout.DEFAULT );
        assertEquals(
                Boolean.TRUE,
                clientResource2.exists( "jason" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        assertEquals(
                Boolean.FALSE,
                clientResource2.exists( "barry" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");

        assertEquals(
                Boolean.FALSE,
                clientResource1.exists( "jason" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        server.shutdown();
    }
    
   @Test
    public void testQuiescingWorksWhenInstanceNamesAreNotUsed()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource1 = new UserResourceImpl();
        final UserResource clientResource1 = client.getRpcResource( 
                UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource1 );
        
        clientResource1.createUser( "barry", "emailbarry" ).get( Timeout.DEFAULT );

        assertThrows(RpcProxyException.class, () -> {
            clientResource1.quiesceAndPrepareForShutdown(false).get(Timeout.DEFAULT);
        });


        clientResource1.quiesceAndPrepareForShutdown( true ).get( Timeout.DEFAULT );
        
        try
        {
            clientResource1.createUser( "barry2", "emailbarry2" ).get( Timeout.DEFAULT );
            fail( "Should throw exception." );
        }
        catch ( final RpcProxyException ex )
        {
            assertEquals(
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT.getHttpResponseCode(),
                    ex.getFailureType().getHttpResponseCode(),
                    "Shoulda thrown exception.");
        }
        
        server.shutdown();
    }
    
   @Test
    public void testQuiescingWorksWhenInstanceNamesAreUsed()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource1 = new UserResourceImpl();
        final UserResource clientResource1 = client.getRpcResource( 
                UserResource.class, "resource1", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        final UserResource resource2 = new UserResourceImpl();
        final UserResource clientResource2 = client.getRpcResource(
                UserResource.class, "resource2", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( "resource1", resource1 );
        server.register( "resource2", resource2 );
        
        clientResource1.createUser( "barry", "emailbarry" ).get( Timeout.DEFAULT );
        clientResource2.createUser( "jason", "emailjason" ).get( Timeout.DEFAULT );

        assertThrows(RpcProxyException.class, () -> {
            clientResource1.quiesceAndPrepareForShutdown( false ).get( Timeout.DEFAULT );
        });

        clientResource1.quiesceAndPrepareForShutdown( true ).get( Timeout.DEFAULT );
        
        try
        {
            clientResource1.createUser( "barry2", "emailbarry2" ).get( Timeout.DEFAULT );
            fail( "Should throw exception." );
        }
        catch ( final RpcProxyException ex )
        {
            assertEquals(
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT.getHttpResponseCode(),
                    ex.getFailureType().getHttpResponseCode(),
                    "Shoulda thrown exception.");
        }
        clientResource2.createUser( "barry2", "emailbarry2" ).get( Timeout.DEFAULT );
        
        server.shutdown();
    }
    
   @Test
    public void testIllegalNullInRpcRequestNotAllowed()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource = new UserResourceImpl();
        final UserResource clientResource = client.getRpcResource(
                UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );

        assertThrows(IllegalArgumentException.class, () -> {
            clientResource.getNullInteger( null );
        });

        clientResource.getNullIllegally( null );
        
        server.shutdown();
    }
    
   @Test
    public void testIllegalNullInRpcResponseNotAllowed()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource = new UserResourceImpl();
        final UserResource clientResource = client.getRpcResource(
                UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );

        assertThrows(RpcProxyException.class, () -> {
            clientResource.getNullIllegally( Integer.valueOf( 1 ) ).get( Timeout.DEFAULT );
        });

        clientResource.getNullInteger( Integer.valueOf( 1 ) ).get( Timeout.DEFAULT );
        
        server.shutdown();
    }
    
   @Test
    public void testIllegalNonNullInRpcResponseNotAllowed()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );

        try {
            final UserResource resource = new UserResourceImpl();
            final UserResource clientResource = client.getRpcResource(
                    UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1
            );

            server.register(null, resource);
            assertThrows(RpcProxyException.class, () -> {
                clientResource.getNonNullIllegally(Integer.valueOf(1)).get(Timeout.DEFAULT);
            });

        } finally {
            // Ensure these always get called
            client.shutdown();
            server.shutdown();
        }
    }
    
   @Test
    public void testComplexPayloadsAreHandled()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final ComplexPayloadResource resource = new ComplexPayloadResourceImpl();
        final ComplexPayloadResource clientResource = client.getRpcResource(
                ComplexPayloadResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );
        
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable.");
        
        final ComplexPayload payload = BeanFactory.newBean( ComplexPayload.class );
        payload.setNested5( new String [] { "b" } );
        payload.setNested2( (ComplexPayload[]) Array.newInstance( ComplexPayload.class, 0 ) );
        clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT );
        
        final ComplexPayload nestedPayload = BeanFactory.newBean( ComplexPayload.class );
        nestedPayload.setNested2( (ComplexPayload[])Array.newInstance( ComplexPayload.class, 0 ) );
        nestedPayload.setNested5( new String [] { "a" } );

        payload.setNested2( new ComplexPayload [] { nestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        payload.setNested3( new ComplexPayload [] { nestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        payload.setNested4( new ComplexPayload [] { nestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        payload.setNested1( nestedPayload );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        
        final ComplexPayload nestedNestedPayload = BeanFactory.newBean( ComplexPayload.class );
        nestedNestedPayload.setNested2( (ComplexPayload[])Array.newInstance( ComplexPayload.class, 0 ) );
        nestedNestedPayload.setNested5( new String [] { "c" } );
        
        payload.getNested1().setNested1( nestedNestedPayload );
        payload.getNested1().setNested2( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested1().setNested3( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested1().setNested4( new ComplexPayload [] { nestedNestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        
        payload.getNested2()[ 0 ].setNested1( nestedNestedPayload );
        payload.getNested2()[ 0 ].setNested2( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested2()[ 0 ].setNested3( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested2()[ 0 ].setNested4( new ComplexPayload [] { nestedNestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        
        payload.getNested3()[ 0 ].setNested1( nestedNestedPayload );
        payload.getNested3()[ 0 ].setNested2( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested3()[ 0 ].setNested3( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested3()[ 0 ].setNested4( new ComplexPayload [] { nestedNestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        
        payload.getNested4()[ 0 ].setNested1( nestedNestedPayload );
        payload.getNested4()[ 0 ].setNested2( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested4()[ 0 ].setNested3( new ComplexPayload [] { nestedNestedPayload } );
        payload.getNested4()[ 0 ].setNested4( new ComplexPayload [] { nestedNestedPayload } );
        assertPayloadReturnedMatchesPayloadSent(
                payload,
                clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT ) );
        
        nestedNestedPayload.setNested5( new String [] {} );

        assertThrows(DaoException.class, () -> {
            clientResource.getComplexPayload( payload ).get( Timeout.DEFAULT );
        });

        client.shutdown();
        server.shutdown();
    }
    
    
    private void assertPayloadReturnedMatchesPayloadSent( 
            final ComplexPayload payload1, 
            final ComplexPayload payload2 )
    {
        if ( null == payload1 && null == payload2 )
        {
            return;
        }
        if ( null == payload1 || null == payload2 )
        {
            throw new RuntimeException( "Payload 1 and payload2 aren't the same." );
        }
        
        assertPayloadReturnedMatchesPayloadSent( payload1.getNested1(), payload2.getNested1() );
        assertPayloadReturnedMatchesPayloadSent( payload1.getNested2(), payload2.getNested2() );
        assertPayloadReturnedMatchesPayloadSent( payload1.getNested3(), payload2.getNested3() );
        assertPayloadReturnedMatchesPayloadSent( payload1.getNested4(), payload2.getNested4() );
    }
    
    
    private void assertPayloadReturnedMatchesPayloadSent( 
            final ComplexPayload [] payload1, 
            final ComplexPayload [] payload2 )
    {
        if ( ( null == payload1 || 0 == payload1.length ) && ( null == payload2 || 0 == payload2.length ) )
        {
            return;
        }
        if ( null == payload1 || null == payload2 )
        {
            throw new RuntimeException( "Payload 1 and payload2 aren't the same." );
        }
        if ( payload1.length != payload2.length )
        {
            throw new RuntimeException( "Payload 1 and payload2 aren't the same." );
        }
        for ( int i = 0; i < payload1.length; ++i )
        {
            assertPayloadReturnedMatchesPayloadSent( payload1[ i ], payload2[ i ] );
        }
    }
    
   @Test
    public void testRpcClientInvocationDiagnosticsCorrectlyTracksInvocationsOnceEnabled()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource1 = new UserResourceImpl();
        final UserResource clientResource1a = client.getRpcResource( 
                UserResource.class, "resource1", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        RpcClientInvocationDiagnostics.getInstance().enable();
        final UserResource clientResource1b = client.getRpcResource( 
                UserResource.class, "resource1", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        final UserResource resource2 = new UserResourceImpl();
        final UserResource resource3 = new UserResourceImpl();
        final UserResource clientResource2a = client.getRpcResource(
                UserResource.class, "resource2", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        final UserResource clientResource2b = client.getRpcResource(
                UserResource.class, "resource2", ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        final UserResource clientResource3 = client.getRpcResource(
                UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( "resource1", resource1 );
        server.register( "resource2", resource2 );
        server.register( null, resource3 );
        
        assertTrue(
                clientResource1a.isServiceable(),
                "Shoulda been serviceable.");
        assertTrue(
                clientResource1b.isServiceable(),
                "Shoulda been serviceable.");
        clientResource1a.createUser( "barry", "emailbarry" ).get( Timeout.DEFAULT );
        assertEquals(
                Boolean.FALSE,
                clientResource1a.exists( "jason" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        assertEquals(
                Boolean.TRUE,
                clientResource1a.exists( "barry" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        
        assertTrue(
                clientResource2a.isServiceable(),
                "Should be serviceable.");
        clientResource2a.createUser( "jason", "emailjason" ).get( Timeout.DEFAULT );
        assertEquals(
                Boolean.TRUE,
                clientResource2a.exists( "jason" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        assertEquals(
                Boolean.FALSE,
                clientResource2b.exists( "barry" ).get( Timeout.DEFAULT ),
                "Should receive correct response.");
        
        assertEquals(
                Boolean.FALSE,
                clientResource1a.exists( "jason" ).get( Timeout.DEFAULT ),
                "Shoulda received correct response.");
        assertEquals(
                Boolean.TRUE,
                clientResource1a.exists( "barry" ).get( Timeout.DEFAULT ),
                "Shoulda received correct response.");
        
        assertTrue(
                clientResource3.isServiceable(),
                "Shoulda been serviceable.");
        assertTrue(
                clientResource3.isServiceable(),
                "Shoulda been serviceable.");
        server.shutdown();
        
        assertEquals(
                1,
                RpcClientInvocationDiagnostics.getInstance().getBtih( UserResource.class, "resource1" )
                        .getTotalCallCount(),
                "Shoulda made the single isServiceable invocation on clientResource1b");
        assertEquals(
                2,
                RpcClientInvocationDiagnostics.getInstance().getBtih( UserResource.class, null )
                        .getTotalCallCount(),
                "Shoulda made the 2 isServiceable invocations on clientResource3");
        assertThrows(IllegalStateException.class, () -> {
            RpcClientInvocationDiagnostics.getInstance().getBtih(
                    UserResource.class, "resource2" );
        });

        final List<BasicTestsInvocationHandler> btihs =
                RpcClientInvocationDiagnostics.getInstance().getBtihs( UserResource.class, "resource2" );
        assertEquals(
                3,
                btihs.get( 0 ).getTotalCallCount(),
                "Shoulda reported correct invocation count.");
        assertEquals(
                1,
                btihs.get( 1 ).getTotalCallCount(),
                "Shoulda reported correct invocation count.");
        assertEquals(
                null,
                RpcClientInvocationDiagnostics.getInstance().getBtih(
                        MortgageResource.class, "resource1" ),
                "Shoulda returned null since resource not registered.");
        assertEquals(
                null,
                RpcClientInvocationDiagnostics.getInstance().getBtihs(
                        MortgageResource.class, "resource1" ),
                "Shoulda returned null since resource not registered.");
        assertEquals(
                null,
                RpcClientInvocationDiagnostics.getInstance().getBtih(
                        UserResource.class, "resource4" ),
                "Shoulda returned null since resource not registered.");
        assertEquals(
                null,
                RpcClientInvocationDiagnostics.getInstance().getBtihs(
                        UserResource.class, "resource4" ),
                "Shoulda returned null since resource not registered.");
    }
    
   @Test
    public void testReceivingAnInvalidResponsePayloadResultsInInternalErrorFailureFromRpcClient() {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl("localhost", port);
        final RpcServer server = new RpcServerImpl(port);
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1);

        server.register("", resource);

        assertTrue(clientResource.isServiceable(),
                "Shoulda been serviceable.");
        DaoException ex = assertThrows(DaoException.class, () -> {
            clientResource.getAllMortgagesWithBadResponse(null).get(Timeout.DEFAULT);
        }, "Shoulda thrown an internal error exception.");
        assertEquals(GenericFailure.INTERNAL_ERROR, ex.getFailureType());
    }
   @Test
    public void testRpcServerShutdownResultingInTcpIpConnectionCloseResultsInClientUnserviceableErrors()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResourceImpl resource = new MortgageResourceImpl();
        resource.setSimulatedDelay( 10000 );
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );
        
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable."
                );
        
        final RpcFuture< ? > future = clientResource.getAllMortgages();
        server.shutdown();
        
        final Set< Class< ? extends Throwable > > s = new HashSet<>();
        s.add( RpcRequestUnserviceableException.class );
        s.add( RpcProxyException.class ); 
        
        TestUtil.assertThrows( 
                "TCP/IP channel close shoulda thrown exception.",
                s, 
                new TestUtil.BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        future.get( Timeout.DEFAULT );
                    }
                } );
        client.shutdown();
    }
    
   @Test
    public void testTryingToSendAnInvalidRequestPayloadResultsInBadRequestFailureFromRpcClient()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );
        
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable."
                 );
        
        final AllMortgagesResponse request = BeanFactory.newBean( AllMortgagesResponse.class );
        final Mortgage mortgage = BeanFactory.newBean( Mortgage.class );
        request.setMortgages( new Mortgage[] { mortgage } );
        TestUtil.assertThrows(
                "Shoulda thrown a bad request exception.",
                GenericFailure.BAD_REQUEST,
                new TestUtil.BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        clientResource.getAllMortgagesWithBadResponse( request ).get( Timeout.DEFAULT );
                    }
                });
        server.shutdown();
        client.shutdown();
    }
    
   @Test
    public void testSendingAnInvalidRequestPayloadResultsInBadRequestFailureFromRpcServer()
            throws NetworkConnectionClosedException, NetworkConnectionClosedException {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();

        server.register( null, resource );
        
        final AllMortgagesResponse requestPayload = BeanFactory.newBean( AllMortgagesResponse.class );
        final Mortgage mortgage = BeanFactory.newBean( Mortgage.class );
        requestPayload.setMortgages( new Mortgage[] { mortgage } );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler<JsonNetworkMessage> messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, btih );
        final TcpIpClient<JsonNetworkMessage> client = new TcpIpClientImpl<>(
                "localhost", 
                port, 
                messageHandler, 
                new JsonNetworkMessageDecoder(),
                new JsonNetworkMessageEncoder(),
                null );
        client.run();
        
        final Request request = BeanFactory.newBean( Request.class )
                .setId( 1 )
                .setMethod( "getMortgageFor" )
                .setType( "MORTGAGE_PROCESSOR" )
                .setParams( "[]" );
        client.send( new JsonNetworkMessage( request.toJson() ) );
        
        int i = 1000;
        while ( --i > 0 && 0 == btih.getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "RPC server shoulda said the request was bad."
                 );
        TestUtil.sleep( 100 );
        final Response response = JsonMarshaler.unmarshal(
                Response.class,
                ( (JsonNetworkMessage)btih.getMethodInvokeData().get( 0 ).getArgs().get( 0 ) ).getJson() );
        assertEquals(
                GenericFailure.BAD_REQUEST.getHttpResponseCode(),
                response.getFailure().getHttpResponseCode(),
                "RPC server shoulda said the request was bad."
                 );
        
        server.shutdown();
        client.shutdown();
    }
    
   @Test
    public void testSerialCommandExecutionIsEnforcedProperlyWhenEnforcedByServer()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResourceImpl resource = new MortgageResourceImpl();
        resource.setSimulatedDelay( 400 );
        resource.disableDynamicDelay();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register(
                null, 
                SerialAccessRpcResourceFactory.asSerialResource( MortgageResource.class, resource ) );
        
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable."
                 );
        
        clientResource.getAllMortgages();
        TestUtil.sleep( 100 );
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable."
                 );
        TestUtil.assertThrows( 
                null,
                RpcProxyException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );
        
        int i = 1000;
        while ( --i > 0 )
        {
            TestUtil.sleep( 10 );
            try
            {
                clientResource.getAllMortgages().get( Timeout.DEFAULT );
                server.shutdown();
                client.shutdown();
                return;
            }
            catch ( final RpcProxyException ex )
            {
                Validations.verifyNotNull( "Shut up CodePro", ex );
            }
        }
        fail( "Shoulda eventually been able to make another RPC call." );
    }
    
   @Test
    public void testSerialCommandExecutionIsEnforcedProperlyWhenEnforcedByClient()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResourceImpl resource = new MortgageResourceImpl();
        resource.setSimulatedDelay( 400 );
        resource.disableDynamicDelay();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.SERIALIZED, -1 );

        server.register( null, resource );
        
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable."
                 );
        
        clientResource.getAllMortgages();
        assertTrue(
                clientResource.isServiceable(),
                "Shoulda been serviceable."
                );
        TestUtil.assertThrows( 
                null,
                IllegalStateException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages();
                    }
                } );
        
        int i = 1000;
        while ( --i > 0 )
        {
            TestUtil.sleep( 10 );
            try
            {
                clientResource.getAllMortgages();
                server.shutdown();
                client.shutdown();
                return;
            }
            catch ( final IllegalStateException ex )
            {
                Validations.verifyNotNull( "Shut up CodePro", ex );
            }
        }
        fail( "Shoulda eventually been able to make another RPC call." );
    }
    
   @Test
    public void testRpcMethodsCanOnlyBeCalledWhileTheResourceIsRegistered()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        
        TestUtil.assertThrows( 
                null,
                RpcRequestUnserviceableException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );
        
        server.register( "old", InterfaceProxyFactory.getProxy( 
                MortgageResource.class, 
                new InvocationHandler()
        {
            public Object invoke(
                    final Object proxy, final Method method, final Object[] args ) throws Throwable
            {
                if ( RpcFuture.class == method.getReturnType() )
                {
                    throw new RuntimeException( "I like to misbehave." );
                }
                return null;
            }
        } ) );
        TestUtil.assertThrows( 
                null,
                RpcRequestUnserviceableException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );
     
        // duplicate registration is allowed, the old one will be deleted
        server.register( "old", InterfaceProxyFactory.getProxy( 
                MortgageResource.class, 
                new InvocationHandler()
        {
            public Object invoke(
                    final Object proxy, final Method method, final Object[] args ) throws Throwable
            {
                if ( RpcFuture.class == method.getReturnType() )
                {
                    throw new RuntimeException( "I like to misbehave." );
                }
                return null;
            }
        } ) );
        TestUtil.assertThrows( 
                null,
                RpcRequestUnserviceableException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );
     
        // The newest registered resource should be used for RPC servicing
        server.register( "", resource );
        clientResource.getAllMortgages().get( Timeout.DEFAULT );
        clientResource.getAllMortgages().get( Timeout.DEFAULT );

        server.unregister( "", MortgageResource.class );
        clientResource.getAllMortgages();
        TestUtil.assertThrows( 
                null,
                RpcRequestUnserviceableException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );
        
        server.unregister( "old", MortgageResource.class );
        
        TestUtil.assertThrows( 
                null,
                RpcRequestUnserviceableException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );

        TestUtil.assertThrows( 
                null,
                IllegalStateException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        server.unregister( "singleton", MortgageResource.class );
                    }
                } );
        
        server.register( "", resource );
        clientResource.getAllMortgages().get( Timeout.DEFAULT );
        server.shutdown();
        client.shutdown();
    }
    
   @Test
    public void testRpcCallsReturnValueReturned()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        server.register( null, resource );
        
        final AllMortgagesResponse allMortgages = clientResource.getAllMortgages().get( Timeout.DEFAULT );
        assertEquals(
                3,
                allMortgages.getMortgages().length,
                "Shoulda returned all mortgages."
                );
        assertEquals(
                BaseMortgageResource.MORTGAGE1_ID,
                allMortgages.getMortgages()[ 0 ].getMortgageId(),
                "Shoulda returned all mortgages."
                 );
        
        final UUID mid =
                clientResource.getMortgageFor( BaseMortgageResource.BARRY_ID ).get( Timeout.DEFAULT );
        assertEquals(
                BaseMortgageResource.MORTGAGE3_ID,
                mid,
                "Shoulda returned correct mortgage id."
                );
        
        assertEquals(
                null,
                clientResource.getMortgageFor( UUID.randomUUID() ).get( Timeout.DEFAULT ),
                "Shoulda returned null returned by actual resource."
                 );
        server.shutdown();
        client.shutdown();
    }
    
    
   @Test
    public void testRpcCompletedListenerGetsCalledWhenRpcCompletes()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        server.register( null, resource );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final RpcCompletedListener< Object > listener =
                InterfaceProxyFactory.getProxy( RpcCompletedListener.class, btih );
        final Method methodCompleted = 
                ReflectUtil.getMethod( RpcCompletedListener.class, "remoteProcedureRequestCompleted" );
        client.addRpcCompletedListener( listener );
        
        final AllMortgagesResponse allMortgages = clientResource.getAllMortgages().get( Timeout.DEFAULT );
        assertEquals(3,
                allMortgages.getMortgages().length,
                "Shoulda returned all mortgages."
                );
        assertEquals(BaseMortgageResource.MORTGAGE1_ID,
                allMortgages.getMortgages()[ 0 ].getMortgageId(),
                "Shoulda returned all mortgages."
                 );
        
        final UUID mid = 
                clientResource.getMortgageFor( BaseMortgageResource.BARRY_ID ).get( Timeout.DEFAULT );
        assertEquals( mid,
                BaseMortgageResource.MORTGAGE3_ID,
                "Shoulda returned correct mortgage id."
                );
        
        assertEquals(null,
                clientResource.getMortgageFor( UUID.randomUUID() ).get( Timeout.DEFAULT ),
                "Shoulda returned null returned by actual resource."
                 );
        
        int i = 100;
        while ( --i > 0 && 4 > btih.getMethodInvokeData( methodCompleted ).size() )
        {
            TestUtil.sleep( 10 );
        }
        
        final List<BasicTestsInvocationHandler.MethodInvokeData> invokeData = btih.getMethodInvokeData( methodCompleted );
        assertTrue( allMortgages ==
                        ( (RpcFuture< ? >)invokeData.get( 1 ).getArgs().get( 0 ) ).getWithoutBlocking(),
                "Shoulda provided futures for every invoked method."
                );
        assertTrue(mid ==
                        ( (RpcFuture< ? >)invokeData.get( 2 ).getArgs().get( 0 ) ).getWithoutBlocking(),
                "Shoulda provided futures for every invoked method."
                 );
        assertTrue(null ==
                        ( (RpcFuture< ? >)invokeData.get( 3 ).getArgs().get( 0 ) ).getWithoutBlocking(),
                "Shoulda provided futures for every invoked method."
                 );
        server.shutdown();
        client.shutdown();
    }
    

   @Test
    public void testRpcCallsThrowExceptionIfThrown()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource( 
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        server.register( null, resource );
        
        final RpcFuture< UUID > future = clientResource.getMortgageFor( BaseMortgageResource.JASON_ID );
        try
        {
            future.get( Timeout.DEFAULT );
            fail( "Shoulda thrown exception." );
        }
        catch ( final RpcProxyException ex )
        {
            assertEquals(  NamingConventionType.CONSTANT.convert( GenericFailure.MULTIPLE_RESULTS_FOUND.getCode() ),
                    ex.getFailureType().getCode(),
                    "Shoulda sent back correct error info."
                   );
            assertEquals(GenericFailure.MULTIPLE_RESULTS_FOUND.getHttpResponseCode(),
                    ex.getFailureType().getHttpResponseCode(),
                    "Shoulda sent back correct error info."
                     );
        }
        
        server.shutdown();
        client.shutdown();
    }
    

   @Test
    public void testRpcMethodsThatDoNotReturnAnRpcFutureCannotBeInvokedByClient()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource( 
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );
        clientResource.getAllMortgages().get( Timeout.DEFAULT );
        
        TestUtil.assertThrows( 
                null, 
                UnsupportedOperationException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource
                          .isBadMethodThatDoesNotReturnRpcFutureAndThusCannotBeCalledByRpcClient();
                    }
                } );
        server.shutdown();
        client.shutdown();
    }
    

   @Test
    public void testRpcServerShutdownWorksEvenIfOutstandingRequests()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        
        TestUtil.assertThrows( 
                null,
                RpcRequestUnserviceableException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.getAllMortgages().get( Timeout.DEFAULT );
                    }
                } );
        
        server.register( null, resource );
        clientResource.getAllMortgages();
        server.shutdown();
        client.shutdown();

        TestUtil.assertThrows( 
                null,
                IllegalStateException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        server.register( "singleton", resource );
                    }
                } );
    }
    
   @Test
    public void testInvokingMethodThatReturnsValueWhenShouldNotDetectedAsError()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        
        server.register( null, resource );

        TestUtil.assertThrows( 
                null,
                RpcProxyException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.badMethodThatReturnsSomething().get( Timeout.DEFAULT );
                    }
                } );
        server.shutdown();
        client.shutdown();
    }
    
   @Test
    public void testInvokingMethodThatReturnsWrongValueTypeDetectedAsError()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource( 
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        
        server.register( "", resource );

        TestUtil.assertThrows( 
                null,
                RpcProxyException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        clientResource.badMethodThatReturnsWrongType().get( Timeout.DEFAULT );
                    }
                } );
        server.shutdown();
        client.shutdown();
    }
    
   @Test
    public void testVariableParameterMethodsWork()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final MortgageResource resource = new MortgageResourceImpl();
        final MortgageResource clientResource = client.getRpcResource(
                MortgageResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );
        
        server.register( "", resource );
        assertEquals(Integer.valueOf( 5 ),
                clientResource.getMax( Integer.valueOf( 5 ) )
                        .get( Timeout.DEFAULT ),
                "Shoulda responded with correct answer."
                 );
        assertEquals( Integer.valueOf( 999 ),
                clientResource.getMax( Integer.valueOf( 5 ), null, null )
                        .get( Timeout.DEFAULT ),
                "Shoulda ignored null variable parameters."
                );
        assertEquals(Integer.valueOf( 5 ),
                clientResource.getMax(2, 3, Integer.valueOf( 5 ) )
                        .get( Timeout.DEFAULT ),
                "Shoulda responded with correct answer."
                 );
        assertEquals(Integer.valueOf( 5 ),
                clientResource.getMax( Integer.valueOf( 3 ), Integer.valueOf( 5 ) )
                        .get( Timeout.DEFAULT ),
                "Shoulda responded with correct answer."
                 );
        
        assertEquals( 0,
                clientResource.getSum().get( Timeout.DEFAULT ).intValue(),
                "getSum shoulda returned 0 response for null input."
                );
        assertEquals(    0,
                clientResource.getSum( null ).get( Timeout.DEFAULT ).intValue(),
                "getSum shoulda returned 0 response for null input."
             );
        
        final RpcFuture< Integer > future = clientResource.getMax();
        TestUtil.assertThrows( 
                null, 
                RpcProxyException.class, new TestUtil.BlastContainer()
                {
                    public void test()
                    {
                        future.get( Timeout.DEFAULT );
                    }
                } );
        server.shutdown();
        client.shutdown();
    }
    
   @Test
    public void testRpcCallSucceedsIfRpcResourceIsNotInitiallyAvailableButBecomesAvailableWithinTimeout()
    {
       // if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
         RpcClient client = null;
         RpcServer server = null;
        try {

            client = new RpcClientImpl("localhost", port);
            final UserResource resource = new UserResourceImpl();

            final boolean[] success = new boolean[1];
            final UserResource clientResource = client.getRpcResource(
                    UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, 5000);
            SystemWorkPool.getInstance().submit(new Runnable() {
                public void run() {
                    clientResource.getCount().get(Timeout.DEFAULT);
                    success[0] = true;
                }
            });

            TestUtil.sleep(new SecureRandom().nextInt(400));
             server = new RpcServerImpl(port);
            TestUtil.sleep(new SecureRandom().nextInt(800));
            assertFalse(success[0]);
            server.register(null, resource);

            int i = 1000;
            while (--i > 0 && !success[0]) {
                TestUtil.sleep(10);
            }

            assertTrue(success[0],
                    "Request shoulda eventually gone through."
                    );

        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage());
        } finally {
            // Ensure resources are always cleaned up
            if (server != null) {
                server.shutdown();
            }
            if (client != null) {
                client.shutdown();
            }
        }
    }
    
   @Test
    public void testPingAndIsServiceableWorks()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
         RpcClient client = null;
         RpcServer server = null;
        try {
             client = new RpcClientImpl("localhost", port);
            final UserResource resource = new UserResourceImpl();
            final UserResource clientResource = client.getRpcResource(
                    UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1);
            final UserResource clientResource2 = client.getRpcResource(
                    UserResource.class, "invalid", ConcurrentRequestExecutionPolicy.CONCURRENT, -1);
            assertFalse( clientResource.isServiceable(),
                    "Should notta been serviceable."
                   );

            final Duration duration = new Duration();
             server = new RpcServerImpl(port);
            assertFalse(clientResource.isServiceable(),
                    "Should notta been serviceable."
                    );
            assertTrue(500 < duration.getElapsedMillis(),
                    "Shoulda waited a while before trying to connect again to a server that wasn't serviceable."
                    );

            server.register(null, resource);

            // Wait for the server to start before proceeding
            TestUtil.sleep(500);

            assertTrue(clientResource.isServiceable(),
                    "Shoulda been serviceable."
                    );
            assertFalse(clientResource2.isServiceable(),
                    "Should notta been serviceable."
                    );

            clientResource.ping().get(Timeout.DEFAULT);
            TestUtil.assertThrows(
                    null,
                    RpcRequestUnserviceableException.class, new TestUtil.BlastContainer() {
                    public void test() {
                        clientResource2.ping().get(Timeout.DEFAULT);
                    }
                    });


        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage());
        } finally {
            // Ensure cleanup
            if (server != null) {
                server.shutdown();
            }
            if (client != null) {
                client.shutdown();
            }
        }
    }
    
   @Test
    public void testLargeRpcRequestsCanBeSentAndReceived()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final RpcClient client = new RpcClientImpl( "localhost", port );
        final RpcServer server = new RpcServerImpl( port );
        final UserResource resource = new UserResourceImpl();
        final UserResource clientResource = client.getRpcResource(
                UserResource.class, null, ConcurrentRequestExecutionPolicy.CONCURRENT, -1 );

        server.register( null, resource );
        
        assertTrue( clientResource.isServiceable(),
                "Shoulda been serviceable."
                 );
        clientResource.createUser( "barry", getLargePayload() ).get( Timeout.DEFAULT );
        assertEquals( Boolean.FALSE,
                clientResource.exists( "jason" ).get( Timeout.DEFAULT ),
                "Shoulda received correct response."
                );
        assertEquals(Boolean.TRUE,
                clientResource.exists( "barry" ).get( Timeout.DEFAULT ),
                "Shoulda received correct response."
                 );
        server.shutdown();
        client.shutdown();
    }
    
    
    private String getLargePayload()
    {
        final StringBuilder retval = new StringBuilder();
        for ( int i = 0; i < 18; ++i )
        {
            retval.append( retval.toString() + System.currentTimeMillis() );
        }
        final BytesRenderer renderer = new BytesRenderer();
        LOG.info( "Large message payload will be: " + renderer.render( retval.length() * 2 )
                  + " (heap size is " + renderer.render( Runtime.getRuntime().maxMemory() ) + ")" );
        return retval.toString();
    }
    
   @Test
    public void testConcurrentRpcServerAndClientMessaging()
    {
        //if (shouldSkipRpcTests()) return; // This test is failing on the build server, but works locally.  It is likely a RPC port issue.
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final int numberOfResources = 2;
        final int numberOfClients = 4;
        final int numberOfRpcCallerThreadsPerClient = 2;
        final int numberOfRpcCallsPerRpcCallerThread = 10;
        
        // addPaymentForMortgage calls getMortgageFor, so twice as many calls will be reported as are called
        final int numberOfRpcCalls = 
                numberOfRpcCallsPerRpcCallerThread * numberOfRpcCallerThreadsPerClient 
                * numberOfClients * numberOfResources * 2; 
        
        final AtomicInteger numberOfRpcCallsMade = new AtomicInteger( 0 );
        final Map< Integer, Class< ? extends RpcResource> > resourceApis = new HashMap<>();
        final Map< Integer, RpcResource > resources = new HashMap<>();
        for ( int i = 0; i < numberOfResources; ++i )
        {
            try
            {
                @SuppressWarnings( "unchecked" )
                final Class< ? extends RpcResource > rpcResourceApi =
                        (Class< ? extends RpcResource >)
                        Class.forName( getClass().getPackage().getName() + ".mockresource.Resource" + i );
                resourceApis.put(
                        Integer.valueOf( i ),
                        rpcResourceApi );
                
                final BaseMortgageResource resource = (BaseMortgageResource)Class.forName( 
                        getClass().getPackage().getName() + ".mockresource.Resource" + i + "Impl" )
                        .newInstance();
                resource.setCallCounter( numberOfRpcCallsMade );
                resource.setSimulatedDelay( 1 );
                resources.put(
                        Integer.valueOf( i ),
                        resource );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        final RpcServer server = new RpcServerImpl( port );
        for ( int i = 0; i < numberOfResources; ++i )
        {
            server.register( 
                    null,
                    resources.get( Integer.valueOf( i ) ) );
        }

        final CountDownLatch clientStartLatch = new CountDownLatch( 1 );
        final Method methodAddPayment =
                ReflectUtil.getMethod( MortgageResourceMethods.class, "addPaymentForMortgage" );
        final Method methodGetAllMortgages =
                ReflectUtil.getMethod( MortgageResourceMethods.class, "getAllMortgages" );
        final WorkPool wp = WorkPoolFactory.createWorkPool(
                numberOfClients * numberOfRpcCallerThreadsPerClient,
                getClass().getSimpleName() );
        final Set< Object > allClientResources = new HashSet<>();
        for ( int i = 0; i < numberOfClients; ++i )
        {
            final RpcClient client = new RpcClientImpl( "localhost", port );
            final Map< Integer, Object > clientResources = new HashMap<>();
            for ( int j = 0; j < numberOfResources; ++j )
            {
                clientResources.put( 
                        Integer.valueOf( j ),
                        client.getRpcResource( 
                                resourceApis.get( Integer.valueOf( j ) ),
                                null,
                                ConcurrentRequestExecutionPolicy.CONCURRENT, 
                                -1 ) );
            }
            allClientResources.addAll( clientResources.values() );
            for ( int j = 0; j < numberOfRpcCallerThreadsPerClient; ++j )
            {
                wp.submit( new Runnable()
                {
                    public void run()
                    {
                        try
                        {
                            clientStartLatch.await();
                        }
                        catch ( final InterruptedException ex )
                        {
                            throw new RuntimeException( ex );
                        }
                        
                        for ( int k = 0; k < numberOfRpcCallsPerRpcCallerThread; ++k )
                        {
                            for ( final Object clientResource : clientResources.values() )
                            {
                                try
                                {
                                    methodAddPayment.invoke( 
                                            clientResource, 
                                            BaseMortgageResource.BARRY_ID,
                                            BaseMortgageResource.MORTGAGE3_ID, 
                                            Integer.valueOf( 1 ) );
                                }
                                catch ( final Exception ex )
                                {
                                    throw new RuntimeException( ex );
                                }
                            }
                        }
                    }
                } );
            }
        }
        
        final Duration duration = new Duration();
        final Level originalClientLogLevel = RpcLogger.CLIENT_LOG.getLevel();
        final Level originalServerLogLevel = RpcLogger.SERVER_LOG.getLevel();
        RpcLogger.CLIENT_LOG.setLevel( Level.WARN );
        RpcLogger.SERVER_LOG.setLevel( Level.WARN );
        clientStartLatch.countDown();
        final int maxMillis = numberOfRpcCalls * 100;
        while ( numberOfRpcCallsMade.get() != numberOfRpcCalls && duration.getElapsedMillis() < maxMillis )
        {
            TestUtil.sleep( 100 );
        }
        
        assertEquals(numberOfRpcCalls,
                numberOfRpcCallsMade.get(),
                "Shoulda made the number of calls expected."
                 );
        LOG.info( numberOfRpcCallsMade + " RPC calls made in " + duration 
                  + " (requirement to pass was " + maxMillis + "ms)." ); 
        LOG.info( "RPC calls are being serviced at the rate of " 
                  + ( numberOfRpcCalls * 1000 / duration.getElapsedMillis() )
                  + " per second." );

        wp.shutdownNow();
        
        final int mortgageBalance = 10000 - 
                ( numberOfClients * numberOfRpcCallerThreadsPerClient * numberOfRpcCallsPerRpcCallerThread );
        for ( final Object clientResource : allClientResources )
        {
            final AllMortgagesResponse response;
            try
            {
                @SuppressWarnings( "unchecked" )
                final RpcFuture< AllMortgagesResponse > future = 
                    (RpcFuture< AllMortgagesResponse >)methodGetAllMortgages.invoke( clientResource );
                response = future.get( Timeout.DEFAULT );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            
            Mortgage mortgage = null;
            for ( final Mortgage m : response.getMortgages() )
            {
                if ( m.getMortgageId().equals( BaseMortgageResource.MORTGAGE3_ID ) )
                {
                    mortgage = m;
                }
            }
            
            if ( null == mortgage )
            {
                throw new RuntimeException( "Mortgage of interest shoulda been in response." );
            }
            assertEquals(mortgageBalance,
                    mortgage.getRemaining(),
                    "Resource shoulda reflected correct new mortgage total."
                     );
        }
        RpcLogger.CLIENT_LOG.setLevel( originalClientLogLevel );
        RpcLogger.SERVER_LOG.setLevel( originalServerLogLevel );
        server.shutdown();

    }

    private final static Logger LOG = Logger.getLogger( RpcIntegration_Test.class );
}
