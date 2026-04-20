/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessage;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageDecoder;

final class IncomingMessageDecoder< M extends NetworkMessage > extends ByteToMessageDecoder
{
    IncomingMessageDecoder(
            final NetworkMessageDecoder< M > networkMessageDecoder, 
            final Logger log )
    {
        m_networkMessageDecoder = networkMessageDecoder;
        m_log = log;
        Validations.verifyNotNull( "Network message decoder", m_networkMessageDecoder );
        Validations.verifyNotNull( "Log", m_log );
    }
    
    
    @Override
    protected void decode( final ChannelHandlerContext ctx, final ByteBuf in, final List< Object > out )
            throws Exception
    {
        try
        {
            final List< M > decodedMessages = m_networkMessageDecoder.decode( ctx.channel(), in );
            if ( null != decodedMessages )
            {
                out.addAll( decodedMessages );
            }
        }
        catch ( final Throwable t )
        {
            m_log.error( m_networkMessageDecoder.getClass().getSimpleName() 
                         + " failed to decode message.", t );
        }
    }
    
    
    private final NetworkMessageDecoder< M > m_networkMessageDecoder;
    private final Logger m_log;
}