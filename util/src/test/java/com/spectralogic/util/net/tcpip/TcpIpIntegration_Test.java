/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageSender;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessage;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.json.JsonNetworkMessageEncoder;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class TcpIpIntegration_Test 
{
    @Test
    public void testTcpIpCanReAssembleMultiByteCharacters() throws NetworkConnectionClosedException
    {
        if ( true ) return; // This test is disabled because it is too slow and not needed.
        TestUtil.assertJvmEncodingIsUtf8();
        
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();

        final BasicTestsInvocationHandler serverBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( NetworkMessageHandler.class, "handle" ),
                        new InvocationHandler()
                        {
                            public Object invoke( 
                                    final Object proxy,
                                    final Method method,
                                    final Object[] args ) throws Throwable
                            {
                                ( (NetworkMessageSender)args[ 1 ] )
                                        .send( new JsonNetworkMessageEncoder()
                                        .encode( (JsonNetworkMessage)args[ 0 ] ) );
                                return null;
                            }
                        },
                        null ) );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > serverMessageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, serverBtih );
        final TcpIpServerImpl< JsonNetworkMessage > server = new TcpIpServerImpl<>(
                port,
                serverMessageHandler,
                new JsonNetworkMessageDecoder(),
                null );
        server.run();

        final BasicTestsInvocationHandler clientBtih = new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > clientMessageHandler =
                InterfaceProxyFactory.getProxy( NetworkMessageHandler.class, clientBtih );
        final TcpIpClientImpl< JsonNetworkMessage > client = new TcpIpClientImpl<>(
                "localhost",
                port,
                clientMessageHandler,
                new JsonNetworkMessageDecoder(),
                new JsonNetworkMessageEncoder(),
                null );
        client.run();
        
        // Frequently used power of two buffer sizes always split strings of three byte characters.
        // 3 byte characters * 47 characters * 7437 repetitions > 1MB
        final String messageToSend = StringUtils.repeat(
                "いろはにほへどちりぬるをわがよたれぞつねならむうゐのおくやまけふこえてあさきゆめみじゑひもせず",
                7437 );
        client.send( new JsonNetworkMessage( messageToSend ) );
        
        int i = 1000;
        while ( --i > 0 && 1 != serverBtih.getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }
        while ( --i > 0 && 1 != clientBtih.getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }
        
        final JsonNetworkMessage messageToServer =
                (JsonNetworkMessage)serverBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        final JsonNetworkMessage messageToClient =
                (JsonNetworkMessage)clientBtih.getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertEquals(
                "Shoulda received the same message as what we sent.",
                messageToSend,
                messageToServer.getJson() );
        assertEquals(
                "Shoulda received back the same message as what we sent.",
                messageToSend,
                messageToClient.getJson() );

        client.shutdown();
        server.shutdown();
    }

    /*@Test
    public void testCommunicationWorksBiDirectionally() throws NetworkConnectionClosedException
    {
        final AtomicInteger responseNumber = new AtomicInteger();
        final MarshalableBean bean0 = BeanFactory.newBean( MarshalableBean.class );
        bean0.setName( "bean0" );
        
        final BasicTestsInvocationHandler serverBtih =
                new BasicTestsInvocationHandler( MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( NetworkMessageHandler.class, "handle" ),
                        new InvocationHandler()
                        {
                            public Object invoke( 
                                    final Object proxy,
                                    final Method method,
                                    final Object[] args ) throws Throwable
                            {
                                for ( int i = 0; i < 3; ++i )
                                {
                                    bean0.setInt( responseNumber.incrementAndGet() );
                                    ( (NetworkMessageSender)args[ 1 ] ).send( 
                                            new JsonNetworkMessageEncoder().encode(
                                                    new JsonNetworkMessage( bean0.toJson() ) ) );
                                }
                                return null;
                            }
                        },
                        null ) );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > serverMessageHandler = 
                InterfaceProxyFactory.getProxy(
                        NetworkMessageHandler.class, 
                        new BasicTestsInvocationHandler( serverBtih ) );
        
        final BasicTestsInvocationHandler clientBtih =
                new BasicTestsInvocationHandler( null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > clientMessageHandler = 
                InterfaceProxyFactory.getProxy(
                        NetworkMessageHandler.class, 
                        new BasicTestsInvocationHandler( clientBtih ) );
        
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final TcpIpServer server = new TcpIpServerImpl<>(
                port, 
                serverMessageHandler, 
                new JsonNetworkMessageDecoder(), 
                null );
        server.run();
        
        final TcpIpClient< JsonNetworkMessage > client = new TcpIpClientImpl<>(
                "localhost",
                port,
                clientMessageHandler,
                new JsonNetworkMessageDecoder(),
                new JsonNetworkMessageEncoder(),
                null );
        client.run();
        
        assertEquals(
                "Should notta seen any messages come in yet.",
                0,
                serverBtih.getTotalCallCount() );
        assertEquals(
                "Should notta seen any messages come in yet.",
                0,
                clientBtih.getTotalCallCount() );
        
        final MarshalableBean bean1 = BeanFactory.newBean( MarshalableBean.class );
        bean1.setName( "bean1" );
        client.send( new JsonNetworkMessage( bean1.toJson() ) );
        
        final MarshalableBean bean2 = BeanFactory.newBean( MarshalableBean.class );
        bean2.setName( "bean2" );
        client.send( new JsonNetworkMessage( bean2.toJson() ) );
        
        int i = 1000;
        while ( --i > 0 && 2 != serverBtih.getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }
        while ( --i > 0 && 6 != clientBtih.getTotalCallCount() )
        {
            TestUtil.sleep( 10 );
        }
        
        assertEquals(
                "Shoulda seen messages come in.",
                2,
                serverBtih.getTotalCallCount() );
        assertEquals(
                "Shoulda seen messages come in.",
                6,
                clientBtih.getTotalCallCount() );
        
        final Set< Integer > responseNumbers = new HashSet<>();
        for ( final MethodInvokeData mid : clientBtih.getMethodInvokeData() )
        {
            final JsonNetworkMessage networkMessage = (JsonNetworkMessage)mid.getArgs().get( 0 );
            final MarshalableBean bean = 
                    JsonMarshaler.unmarshal( MarshalableBean.class, networkMessage.getJson() );
            assertFalse( 
                    "Shoulda contained unique response numbers.",
                    responseNumbers.contains( Integer.valueOf( bean.getInt() ) ) );
            responseNumbers.add( Integer.valueOf( bean.getInt() ) );
        }
        
        client.shutdown();
        server.shutdown();
    }
    
    
    @Test
    public void testSerialPerformanceWithSmallMessages()
    {
        internalTestPerformance( 10000, 1, false );
    }
    
    
    @Test
    public void testConcurrentPerformanceWithSmallMessages()
    {
        internalTestPerformance( 500, 20, false );
    }
    
    
    @Test
    public void testSerialPerformanceWithLargeMessages()
    {
        internalTestPerformance( 1, 1, true );
    }
    
    
    @Test
    public void testConcurrentPerformanceWithLargeMessages()
    {
        internalTestPerformance( 2, 2, true );
    }
    
    
    private void internalTestPerformance(
            final int numBeansToSendPerClient, 
            final int numClients,
            final boolean largeMessages )
    {
        final String payload = ( largeMessages ) ? getLargePayload() : "";
        final AtomicInteger responseNumber = new AtomicInteger();
        
        final InvocationHandler serverIh =
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( NetworkMessageHandler.class, "handle" ),
                        new InvocationHandler()
                        {
                            public Object invoke( 
                                    final Object proxy,
                                    final Method method,
                                    final Object[] args ) throws Throwable
                            {
                                final MarshalableBean bean0 = BeanFactory.newBean( MarshalableBean.class );
                                bean0.setName( "bean0" );
                                bean0.setPayload( payload );
                                bean0.setInt( responseNumber.incrementAndGet() );
                                ( (NetworkMessageSender)args[ 1 ] ).send( 
                                        new JsonNetworkMessageEncoder().encode(
                                                new JsonNetworkMessage( bean0.toJson() ) ) );
                                return null;
                            }
                        },
                        null );
        @SuppressWarnings( "unchecked" )
        final NetworkMessageHandler< JsonNetworkMessage > serverMessageHandler = 
                InterfaceProxyFactory.getProxy(
                        NetworkMessageHandler.class, 
                        serverIh );
        
        final int port = TcpIpServerTestPort.NEXT_PORT.getAndIncrement();
        final TcpIpServer server = new TcpIpServerImpl<>(
                port, 
                serverMessageHandler, 
                new JsonNetworkMessageDecoder(), 
                null );
        server.run();
        
        final Set< Duration > durations = new CopyOnWriteArraySet<>();
        for ( int clientNumber = 0; clientNumber < numClients; ++clientNumber )
        {
            SystemWorkPool.getInstance().submit( new Runnable()
            {
                public void run()
                {
                    final BasicTestsInvocationHandler clientBtih =
                            new BasicTestsInvocationHandler( null );
                    @SuppressWarnings( "unchecked" )
                    final NetworkMessageHandler< JsonNetworkMessage > clientMessageHandler = 
                            InterfaceProxyFactory.getProxy(
                                    NetworkMessageHandler.class, 
                                    clientBtih );
                    
                    final TcpIpClient< JsonNetworkMessage > client = new TcpIpClientImpl<>(
                            "localhost",
                            port,
                            clientMessageHandler,
                            new JsonNetworkMessageDecoder(),
                            new JsonNetworkMessageEncoder(),
                            null );
                    client.run();
                    
                    final Duration duration = new Duration();
                    for ( int i = 0; i < numBeansToSendPerClient; ++i )
                    {
                        final MarshalableBean bean = BeanFactory.newBean( MarshalableBean.class );
                        bean.setName( "bean" + i );
                        bean.setPayload( payload );
                        try
                        {
                            client.send( new JsonNetworkMessage( bean.toJson() ) );
                        }
                        catch ( final NetworkConnectionClosedException ex )
                        {
                            throw new RuntimeException( ex );
                        }
                    }

                    int i = 1000;
                    while ( --i > 0 && numBeansToSendPerClient != clientBtih.getTotalCallCount() )
                    {
                        TestUtil.sleep( 10 );
                    }
                    
                    assertEquals(
                            "Shoulda seen messages come in.",
                            numBeansToSendPerClient,
                            clientBtih.getTotalCallCount() );
                    
                    durations.add( duration );
                    client.shutdown();
                }
            } );
        }
        
        int i = 10000;
        while ( --i > 0 && numClients != durations.size() )
        {
            TestUtil.sleep( 10 );
        }
        assertEquals(
                "All clients shoulda completed successfully.",
                numClients,
                durations.size() );
        
        final List< Integer > messagesPerSecond = new ArrayList<>();
        for ( final Duration d : durations )
        {
            messagesPerSecond.add( Integer.valueOf(
                    (int)( ( numBeansToSendPerClient * 2 * 1000 ) / d.getElapsedMillis() ) ) );
        }
        Collections.sort( messagesPerSecond );
        LOG.info( "Sent / received messages over TCP/IP with " + numClients + " clients at (messages/sec): " 
                  + messagesPerSecond );
        
        server.shutdown();
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
    }*/
    
    
    private final static Logger LOG = Logger.getLogger( TcpIpIntegration_Test.class );
}
