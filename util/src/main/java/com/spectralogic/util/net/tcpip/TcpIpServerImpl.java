/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageDecoder;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import com.spectralogic.util.tunables.Tunables;

public final class TcpIpServerImpl< M extends NetworkMessage > extends BaseShutdownable implements TcpIpServer
{    
    /**
     * Almost always the only constructor on this class that should be used in
     * code that will be used in production. For example, if you're not sure if
     * this is the construtor you should use, then it <em>is</em> absolutely the
     * constructor you need.
     */
    public TcpIpServerImpl(
            final int port, 
            final NetworkMessageHandler< M > networkMessageHandler,
            final NetworkMessageDecoder< M > networkMessageDecoder,
            final Logger log )
    {
        this( port, networkMessageHandler, networkMessageDecoder, log, 10000 );
    }
    
    
    /**
     * Mostly intended for internal and test-only use, but could possibly
     * find a rare production purpose.
     */
    public TcpIpServerImpl(
            final int port, 
            final NetworkMessageHandler< M > networkMessageHandler,
            final NetworkMessageDecoder< M > networkMessageDecoder,
            final Logger log,
            final int maxRetrySleepDelayMillis )
    {
        Validations.verifyInRange( "Port", 1, 65535, port );
        Validations.verifyNotNull( "Network message handler", networkMessageHandler );
        Validations.verifyNotNull( "Network message decoder", networkMessageDecoder );
        
        final int numThreads = Tunables.tcpIpServerNumThreads();
        m_port = port;
        m_networkMessageHandler = networkMessageHandler;
        m_networkMessageDecoder = networkMessageDecoder;
        m_log = ( null == log ) ? Logger.getLogger( TcpIpServerImpl.class ) : log;
        m_maxRetrySleepDelay = maxRetrySleepDelayMillis;
        m_bossThreadGroup = new NioEventLoopGroup( numThreads );
        m_workerThreadGroup = new NioEventLoopGroup( numThreads );
        
        addShutdownListener( new CleanupOnShutdownListener() );
    }
    
    
    private final class CleanupOnShutdownListener extends CriticalShutdownListener
    {
        public void shutdownOccurred()
        {
            if ( null == m_channel )
            {
                return;
            }
            
            m_log.info( getLogMessage( "is shutting down listening for" ) );
            final Future< ? > f = m_channel.close();
            final Future< ? > f1 = m_bossThreadGroup.shutdownGracefully( 
                    2000, 30000, TimeUnit.MILLISECONDS );
            final Future< ? > f2 = m_workerThreadGroup.shutdownGracefully(
                    2000, 30000, TimeUnit.MILLISECONDS );
            SystemWorkPool.getInstance().submit( new Runnable()
            {
                public void run()
                {
                    try
                    {
                        f.get();
                        f1.get();
                        f2.get();
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException( "Failed to shut down cleanly.", ex );
                    }
                    finally
                    {
                        m_shutdownCompletedLatch.countDown();
                        m_log.info( getLogMessage( "has stopped listening for" ) );
                    }
                }
            } );
        }
    } // end inner class def
    
    
    synchronized public void run()
    {
        verifyNotShutdown();
        int retryInMillis = 50;
        while( true )
        {
            if ( null != m_channel )
            {
                throw new IllegalStateException( "Already started up server." );
            }
            try 
            {
                /* Likely somewhere lost deep within ServerBootstrap is just a
                 * workhorse instance of Java ServerSocket. The JavaDoc for
                 * ServerSocket.setReuseAddress() explians the purpose of this
                 * property. There's an issue during test runs where occassionally
                 * the Simulator's backing TcpIpServerImpl is unable to bind to
                 * the Simulator's default listening port(s), because the port(s)
                 * was just used moments before by a previous test method. In 
                 * case the port was placed in a TIME_WAIT when closed at the end
                 * of the previous test method, and thus the cause of this issue,
                 * we set property reuseAddress to true to hopefully eliminate
                 * the problem or reduce its occurence rate. 
                 */
                final ServerBootstrap bootstrap = new ServerBootstrap()
                    .option( ChannelOption.SO_REUSEADDR, Boolean.TRUE )
                    .group( m_bossThreadGroup, m_workerThreadGroup )
                    .channel( NioServerSocketChannel.class )
                    .childHandler( new TcpIpChannelInitializer() )
                    .childOption( ChannelOption.SO_KEEPALIVE, Boolean.TRUE )
                    .option( ChannelOption.SO_SNDBUF, Integer.valueOf( TCP_BUFFER_SIZE ) )
                    .option( ChannelOption.SO_RCVBUF, Integer.valueOf( TCP_BUFFER_SIZE ) );
    
                m_channel = bootstrap.bind( m_port ).sync().channel();
                m_log.info( getLogMessage( "listening for" ) );
                break;
            } 
            catch ( final Throwable t )
            { 
                retryInMillis = (int)(1.5 * retryInMillis);
                if( m_maxRetrySleepDelay < retryInMillis )
                {
                    shutdown();
                    throw new IllegalStateException(
                       "Giving up attempting to start server listening to port "
                                                            + m_port + ".", t );
                }
                m_log.info( "Failed to start server on port " + m_port +
                            ". Will try again in " + retryInMillis + "ms.", t );
                try
                {
                    Thread.sleep( retryInMillis );
                }
                catch ( final InterruptedException ex )
                {
                    shutdown();
                    Thread.currentThread().interrupt();
                    throw new RuntimeException( ex );
                }
            } // catch ( Throwable )
        } // while ( true )
    }
    
    
    private String getLogMessage( final String action )
    {
        return m_networkMessageHandler.getClass().getSimpleName() + " " + action
               + " incoming messages on TCP port " + m_port + ".";
    }
    
    
    private final class TcpIpChannelInitializer extends ChannelInitializer< SocketChannel >
    {
        @Override
        public void initChannel( final SocketChannel channel ) throws Exception
        {
            channel.pipeline().addLast( 
                    new IncomingMessageDecoder<>( m_networkMessageDecoder, m_log ), 
                    new IncomingMessageProcessor<>( m_networkMessageHandler, m_log ) );
        }
    } // end inner class def
    
    
    private volatile Channel m_channel;
    
    private final int m_port;
    private final NetworkMessageHandler< M > m_networkMessageHandler;
    private final NetworkMessageDecoder< M > m_networkMessageDecoder;
    private final Logger m_log;
    private final int m_maxRetrySleepDelay;
    
    private final NioEventLoopGroup m_bossThreadGroup;
    private final NioEventLoopGroup m_workerThreadGroup;
    private final CountDownLatch m_shutdownCompletedLatch = new CountDownLatch( 1 );
    
    private final static int TCP_BUFFER_SIZE = 1024 * 1024;
    
}
