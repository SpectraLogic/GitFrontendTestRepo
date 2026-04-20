/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkConnectionClosedException;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageSender;

final class NetworkMessageSenderImpl implements NetworkMessageSender
{
    NetworkMessageSenderImpl( final Channel channel )
    {
        m_channel = channel;
        Validations.verifyNotNull( "Channel", m_channel );
    }


    public void send( final byte[] encodedMessage ) throws NetworkConnectionClosedException
    {
        try
        {
            final ByteBuf buffer = Unpooled.buffer( encodedMessage.length );
            buffer.writeBytes( encodedMessage );
            final ChannelFuture future = m_channel.writeAndFlush( buffer );
            future.sync();
        }
        catch ( final Throwable ex )
        {
            if ( m_channel.isActive() )
            {
                throw new RuntimeException( "Failed to send network message.", ex );
            }
            
            throw new NetworkConnectionClosedException( "Failed to send network message.", ex );
        }
    }
    
    
    private final Channel m_channel;
}
