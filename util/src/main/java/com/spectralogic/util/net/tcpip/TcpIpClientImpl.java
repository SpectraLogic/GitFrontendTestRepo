/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageEncoder;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;

public final class TcpIpClientImpl< M extends NetworkMessage > 
    extends BaseShutdownable implements TcpIpClient< M >
{
    public TcpIpClientImpl(
            final String host,
            final int port, 
            final NetworkMessageHandler< M > networkMessageHandler,
            final NetworkMessageDecoder< M > networkMessageDecoder,
            final NetworkMessageEncoder< M > networkMessageEncoder,
            final Logger log )
    {
        Validations.verifyNotNull( "Host", host );
        Validations.verifyInRange( "Port", 1, 65535, port );
        Validations.verifyNotNull( "Network message handler", networkMessageHandler );
        Validations.verifyNotNull( "Network message decoder", networkMessageDecoder );
        Validations.verifyNotNull( "Network message encoder", networkMessageEncoder );
        
        final int numThreads = Math.min( 8, Math.max( 2, Runtime.getRuntime().availableProcessors() ) );
        m_host = host;
        m_port = port;
        m_networkMessageHandler = networkMessageHandler;
        m_networkMessageDecoder = networkMessageDecoder;
        m_networkMessageEncoder = networkMessageEncoder;
        m_log = ( null == log ) ? Logger.getLogger( TcpIpServerImpl.class ) : log;
        m_workerThreadGroup = new NioEventLoopGroup( numThreads );
        
        addShutdownListener( new CleanupOnShutdownListener() );
    }
    
    
    private final class CleanupOnShutdownListener extends CriticalShutdownListener
    {
        public void shutdownOccurred()
        {
            if ( null == m_channel )
            {
                shutdownThreadPool();
            }
            else
            {
                m_channel.close();
                shutdownThreadPool();
                m_log.info( "Disconnected from " + m_host + ":" + m_port + "." );
            }
        }
        
        private void shutdownThreadPool()
        {
            m_log.info( "Shutting down " + m_workerThreadGroup.executorCount() + " worker threads." );
            m_workerThreadGroup.shutdownGracefully( 2000, 20000, TimeUnit.MILLISECONDS );
        }
    } // end inner class def
    
    
    public void send( final M message ) throws NetworkConnectionClosedException
    {
        verifyNotShutdown();
        if ( null == m_channel )
        {
            throw new IllegalStateException( "Client not yet connected to a server." );
        }

        new NetworkMessageSenderImpl( m_channel ).send( m_networkMessageEncoder.encode( message ) );
    }
    
    
    synchronized public void run()
    {
        if ( null != m_channel )
        {
            throw new IllegalStateException( "Already connected to server." );
        }
        
        try 
        {
            final Bootstrap bootstrap = new Bootstrap()
                .group( m_workerThreadGroup )
                .channel( NioSocketChannel.class )
                .handler( new TcpIpChannelInitializer() )
                .option( ChannelOption.SO_KEEPALIVE, Boolean.TRUE )
                .option( ChannelOption.SO_SNDBUF, Integer.valueOf( TCP_BUFFER_SIZE ) )
                .option( ChannelOption.SO_RCVBUF, Integer.valueOf( TCP_BUFFER_SIZE ) );

            m_channel = bootstrap.connect( m_host, m_port ).sync().channel();
            m_log.info( "Connected to " + m_host + ":" + m_port + "." );
            
            m_channel.closeFuture().addListener( new ChannelClosedListener() );
        } 
        catch ( final Throwable t )
        {
            if (m_channel != null) {
                m_channel.close(); // Ensure the channel is closed on failure
            }
            shutdown();
            throw new RuntimeException("Failed to connect to " + m_host + ":" + m_port + ".", t);
        }
        finally {
            if (m_channel != null && !m_channel.isActive()) {
                m_channel.close(); // Ensure cleanup if the channel is inactive
            }
        }
    }
    
    
    private final class TcpIpChannelInitializer extends ChannelInitializer< NioSocketChannel >
    {
        @Override
        public void initChannel( final NioSocketChannel channel ) throws Exception
        {
            channel.pipeline().addLast(
                    new IncomingMessageDecoder<>( m_networkMessageDecoder, m_log ),
                    new IncomingMessageProcessor<>( m_networkMessageHandler, m_log ) );
        }
    } // end inner class def   
    
    
    private final class ChannelClosedListener implements GenericFutureListener< ChannelFuture >
    {
        public void operationComplete( final ChannelFuture arg0 ) throws Exception
        {
            m_log.info( "Communications channel with " + m_host + ":" + m_port + " closed." );
            if ( !isShutdown() )
            {
                shutdown();
            }
        }
    } // end inner class def
    
    
    private volatile Channel m_channel;
    
    private final String m_host;
    private final int m_port;
    private final NetworkMessageHandler< M > m_networkMessageHandler;
    private final NetworkMessageDecoder< M > m_networkMessageDecoder;
    private final NetworkMessageEncoder< M > m_networkMessageEncoder;
    private final Logger m_log;
    
    private final NioEventLoopGroup m_workerThreadGroup;
    private final static int TCP_BUFFER_SIZE = 1024 * 1024;
}
