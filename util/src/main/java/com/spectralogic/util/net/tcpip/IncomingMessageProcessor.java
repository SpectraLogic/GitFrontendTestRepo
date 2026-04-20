/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageHandler;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageSender;

final class IncomingMessageProcessor< M extends NetworkMessage >
    extends ChannelInboundHandlerAdapter
{
    IncomingMessageProcessor(
            final NetworkMessageHandler< M > networkMessageHandler, 
            final Logger log )
    {
        m_networkMessageHandler = networkMessageHandler;
        m_log = log;
        Validations.verifyNotNull( "Network message handler", m_networkMessageHandler );
        Validations.verifyNotNull( "Log", m_log );
    }
    
    
    @Override
    public void channelRead( final ChannelHandlerContext ctx, final Object msg ) throws Exception
    {
        final NetworkMessageSender msgSender =
                new NetworkMessageSenderImpl( ctx.channel() );
        try
        {
            m_handleMethod.invoke( m_networkMessageHandler, new Object [] { msg, msgSender } );
        }
        catch ( final Throwable t )
        {
            m_log.error( m_networkMessageHandler.getClass().getSimpleName() 
                         + " failed to handle incoming message.", t );
        }
    }

    
    @Override
    public void exceptionCaught( final ChannelHandlerContext ctx, final Throwable cause ) throws Exception
    {
        m_log.error( "Exception occured while reading network message.", cause );
        ctx.close();
    }
    
    
    private final Method m_handleMethod = ReflectUtil.getMethod( NetworkMessageHandler.class, "handle" );
    private final NetworkMessageHandler< M > m_networkMessageHandler;
    private final Logger m_log;
}