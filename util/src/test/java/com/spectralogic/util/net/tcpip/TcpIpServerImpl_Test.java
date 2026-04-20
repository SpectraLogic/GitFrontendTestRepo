/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessage;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageEncoder;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("rpc-integration")
public final class TcpIpServerImpl_Test
{
@Test
    public void testConstructorNegativePortNumberNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
                {
                    new TcpIpServerImpl<>(
                            -1,
                            messageHandler,
                            messageDecoder,
                            null );
                }
            } );
    }
    
@Test
    public void testConstructorZeroPortNumberNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TcpIpServerImpl<>( 
                        0, 
                        messageHandler,
                        messageDecoder,
                        null );
            }
        } );
    }
    
@Test
    public void testConstructorPortNumberTooHighNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
             public void test()
            {
                new TcpIpServerImpl<>( 
                        65536, 
                        messageHandler,
                        messageDecoder,
                        null );
            }
        } );
    }
    
@Test
    public void testConstructorNullMessageHandlerNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TcpIpServerImpl<>( 
                        TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                        null,
                        messageDecoder,
                        null );
            }
        } );
    }
    

@Test
    public void testConstructorNullMessageDecoderNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TcpIpServerImpl<>( 
                        TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                        messageHandler,
                        null,
                        null );
            }
        } );
    }
    
    @Test
    public void testHappyConstruction()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        new TcpIpServerImpl<>( 
                TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                messageHandler,
                messageDecoder,
                null ).shutdown();
    }
    
    @Test
    public void testRunTwiceNotAllowed()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        final TcpIpServer server = new TcpIpServerImpl<>( 
                TcpIpServerTestPort.NEXT_PORT.getAndIncrement(), 
                messageHandler,
                messageDecoder,
                null );
        server.run();        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                server.run();
            }
        } );
        server.shutdown();
    }
    
    @Test
    public void testFailsToStartWhenListeningPortAlreadyInUseAndLetsYouKnowThis()
    {
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< NetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageDecoder< NetworkMessage > messageDecoder =
                InterfaceProxyFactory.getProxy( NetworkMessageDecoder.class, null );
        
        final int portAlreadyInUse = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        
        final TcpIpServer server1 = new TcpIpServerImpl<>( 
                portAlreadyInUse,             // Well, it will be soon enough...
                messageHandler,
                messageDecoder,
                null );
        server1.run();        
        
        final TcpIpServer server2 = new TcpIpServerImpl<>( 
                portAlreadyInUse,             // See...
                messageHandler,
                messageDecoder,
                null,
                1000 );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                server2.run();
            }
        } );
        server1.shutdown();
        server2.shutdown();
    }
    
    @Test
    public void testShutdownPreventsMessagesFromBeingSentOnAlreadyEstablishedConnection() 
            throws NetworkConnectionClosedException
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
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
        server.shutdown(); 
        TestUtil.sleep( 100 );
        final Set< Class< ? extends Throwable > > s = new HashSet<>();
        s.add( IllegalStateException.class );
        s.add( NetworkConnectionClosedException.class );
        TestUtil.assertThrows( null, s, new BlastContainer()
        {
            public void test() throws NetworkConnectionClosedException
            {
                client.send( new JsonNetworkMessage( bean.toJson() ) );
            }
        } );
    }
    
    @Test
    public void testShutdownPreventsNewConnectionsFromBeingMade()
    {
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > messageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, null );
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
        
        server.shutdown();
        TestUtil.sleep( 100 );

        TestUtil.assertThrows( null, Exception.class, new BlastContainer()
        {
            public void test()
                {
                    client.run();
                }
            } );
    }
}
