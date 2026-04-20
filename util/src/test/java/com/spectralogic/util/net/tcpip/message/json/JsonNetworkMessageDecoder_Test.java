/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.json;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.tcpip.message.frmwrk.NetworkMessageDecoder;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class JsonNetworkMessageDecoder_Test 
{
    @Test
    public void testDecoderCanDecodeUTF8CharacterThatCrossesBufferBoundaries()
            throws UnsupportedEncodingException
    {
        TestUtil.assertJvmEncodingIsUtf8();
        
        final byte[] jsonPayload = "ξεσκεπάζω_τὴν_ψυχοφθόρα_βδελυγμία".getBytes( "UTF-8" );

        final NetworkMessageDecoder< JsonNetworkMessage > decoder = new JsonNetworkMessageDecoder();
        final Channel channel = InterfaceProxyFactory.getProxy( Channel.class, null );
        final ByteBuf buffer1 = Unpooled.copiedBuffer( jsonPayload, 0, 3 );
        final List< JsonNetworkMessage > decodeResult1 = decoder.decode( channel, buffer1 );
        assertEquals(new ArrayList<Object>(), decodeResult1, "Shoulda returned an empty list.");

        decoder.decode(
                InterfaceProxyFactory.getProxy( Channel.class, null ),
                Unpooled.copiedBuffer( "abcde".getBytes( "UTF-8" ) ) );

        final ByteBuf buffer2 = Unpooled.copiedBuffer( jsonPayload, 3, jsonPayload.length - 3 );
        final List< JsonNetworkMessage > decodeResult2 = decoder.decode( channel, buffer2 );
        assertEquals(new ArrayList<Object>(), decodeResult2, "Shoulda returned an empty list.");

        final ByteBuf buffer3 = Unpooled.copiedBuffer( new byte[] { 0 } );
        final List< JsonNetworkMessage > decodeResult3 = decoder.decode( channel, buffer3 );
        assertNotNull( decodeResult3,
                "Shoulda returned a list." );
        assertEquals(1,  decodeResult3.size(), "Shoulda exactly one element.");
        final Object expected = CollectionFactory.toList( "ξεσκεπάζω_τὴν_ψυχοφθόρα_βδελυγμία" );
        assertEquals(expected, extractStrings( decodeResult3 ), "Shoulda returned the correctly encoded element.");
    }
    
    
    private static List< String > extractStrings( final List< JsonNetworkMessage > messages )
    {
        final List< String > strings = new ArrayList<>( messages.size() );
        for ( final JsonNetworkMessage message : messages )
        {
            strings.add( message.getJson() );
        }
        return strings;
    }
}
