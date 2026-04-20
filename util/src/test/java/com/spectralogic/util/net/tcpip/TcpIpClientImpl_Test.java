/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessage;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageEncoder;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class TcpIpClientImpl_Test 
{
    @Test
    public void testConstructorNullHostNotAllowed()
    {
        if( true ) return; // TODO: Re-enable this test when the client is ready for use.
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TcpIpClientImpl<>( 
                        null,
                        TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                        messageHandler, 
                        new JsonNetworkMessageDecoder(), 
                        new JsonNetworkMessageEncoder(),
                        null );
            }
        } );
    }
    

   /* @Test
    public void testConstructorNegativePortNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                new TcpIpClientImpl<>( 
                        "localhost",
                        -1, 
                        messageHandler, 
                        new JsonNetworkMessageDecoder(), 
                        new JsonNetworkMessageEncoder(),
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorZeroPortNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                new TcpIpClientImpl<>( 
                        "localhost",
                        0, 
                        messageHandler, 
                        new JsonNetworkMessageDecoder(), 
                        new JsonNetworkMessageEncoder(),
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorPortTooHighNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                new TcpIpClientImpl<>( 
                        "localhost",
                        65536, 
                        messageHandler, 
                        new JsonNetworkMessageDecoder(), 
                        new JsonNetworkMessageEncoder(),
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorNullMessageHandlerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                new TcpIpClientImpl<>( 
                        "localhost",
                        TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                        null, 
                        new JsonNetworkMessageDecoder(), 
                        new JsonNetworkMessageEncoder(),
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorNullMessageDecoderNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                new TcpIpClientImpl<>( 
                        "localhost",
                        TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                        messageHandler, 
                        null, 
                        new JsonNetworkMessageEncoder(),
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorNullMessageEncoderNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                new TcpIpClientImpl<>( 
                        "localhost",
                        TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                        messageHandler, 
                        new JsonNetworkMessageDecoder(), 
                        null,
                        null );
            }
        } );
    }
    

    @Test
    public void testHappyConstruction()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        new TcpIpClientImpl<>( 
                "localhost",
                TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                messageHandler, 
                new JsonNetworkMessageDecoder(), 
                new JsonNetworkMessageEncoder(),
                null );
    }
    
    
    @Test
    public void testRunWhenCannotConnectToServerThrowsException()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        final TcpIpClient< JsonNetworkMessage > client = new TcpIpClientImpl<>( 
                "localhost",
                TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                messageHandler, 
                new JsonNetworkMessageDecoder(), 
                new JsonNetworkMessageEncoder(),
                null );
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                client.run();
            }
        } );
        client.shutdown();
    }
    
    
    @Test
    public void testRunTwiceNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final TcpIpServer server = new TcpIpServerImpl<>( 
                port,
                messageHandler,
                new JsonNetworkMessageDecoder(),
                null );
        server.run();
        
        final TcpIpClient< JsonNetworkMessage > client = new TcpIpClientImpl<>( 
                "localhost",
                port,
                messageHandler, 
                new JsonNetworkMessageDecoder(), 
                new JsonNetworkMessageEncoder(),
                null );
        client.run();
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                client.run();
            }
        } );
        client.shutdown();
        server.shutdown();
    }
    
    
    @Test
    public void testTcpIpClientShutsDownIfTcpIpServerClosesChannel()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler = 
                InterfaceProxyFactory.getProxy(
                        NetworkMessageHandler.class, 
                        null );
        
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final TcpIpServer server = new TcpIpServerImpl<>(
                port, 
                messageHandler, 
                new JsonNetworkMessageDecoder(), 
                null );
        server.run();
        
        final TcpIpClient< JsonNetworkMessage > client = new TcpIpClientImpl<>(
                "localhost",
                port,
                messageHandler,
                new JsonNetworkMessageDecoder(),
                new JsonNetworkMessageEncoder(),
                null );
        client.run();
        
        assertFalse(
                "Server should not have shut down yet.",
                server.isShutdown() );
        assertFalse(
                "Client should not have shut down yet.",
                client.isShutdown() );
        
        server.shutdown();
        assertTrue(
                "Server shoulda shut down.",
                server.isShutdown() );
        
        int i = 1000;
        while ( --i > 0 && !client.isShutdown() )
        {
            TestUtil.sleep( 10 );
        }
        
        assertTrue(
                "Client shoulda shut down when channel was closed.",
                client.isShutdown() );
    }
    

    @Test
    public void testShutdownPreventsMessagesFromBeingSentOnAlreadyEstablishedConnection()
                throws NetworkConnectionClosedException
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final TcpIpServer server = new TcpIpServerImpl<>( 
                port,
                messageHandler,
                new JsonNetworkMessageDecoder(),
                null );
        server.run();
        
        final TcpIpClient< JsonNetworkMessage > client = new TcpIpClientImpl<>( 
                "localhost",
                port,
                messageHandler, 
                new JsonNetworkMessageDecoder(), 
                new JsonNetworkMessageEncoder(),
                null );
        client.run();
        
        final MarshalableBean bean = BeanFactory.newBean( MarshalableBean.class );
        bean.setName( "bean" );
        client.send( new JsonNetworkMessage( bean.toJson() ) );  
        client.shutdown();  
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            @Test
    public void test() throws NetworkConnectionClosedException
            {
                client.send( new JsonNetworkMessage( bean.toJson() ) );
            }
        } );
        server.shutdown();
    }*/
}
