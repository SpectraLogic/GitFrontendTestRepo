/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.json;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageDecoder;

public final class JsonNetworkMessageDecoder implements NetworkMessageDecoder< JsonNetworkMessage >
{
    public List< JsonNetworkMessage > decode( final Channel channel, final ByteBuf in )
    {
        return ( 0 == in.readableBytes() ) ? null : getDecoder( channel ).decode( in );
    }
    
    
    synchronized private DecoderForChannel getDecoder( final Channel channel )
    {
        final DecoderForChannel existing = m_decoders.get( channel );
        if ( null != existing )
        {
            return existing;
        }
        
        final DecoderForChannel decoder = new DecoderForChannel( m_charset );
        m_decoders.put( channel, decoder );
        return decoder;
    }
    
    
    private final Map< Channel, DecoderForChannel > m_decoders = new WeakHashMap<>();
    private final Charset m_charset = Charset.defaultCharset();
}
