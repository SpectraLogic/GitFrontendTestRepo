/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.json;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

final class DecoderForChannel
{
    DecoderForChannel( final Charset charset )
    {
        m_decoder = charset.newDecoder();
    }
    

    public List< JsonNetworkMessage > decode( final ByteBuf in )
    {
        final ByteBuffer intermediateInputBuffer = buildBufferToDecode( in );
        final CharBuffer intermediateOutputBuffer = buildCharBuffer( intermediateInputBuffer.capacity() );
        final CoderResult result = m_decoder.decode(
                intermediateInputBuffer,
                intermediateOutputBuffer,
                false );
        if ( result != CoderResult.UNDERFLOW )
        {
            throw new RuntimeException(
            		"Result was " + result + "." +
            		" Input buffer has " + intermediateInputBuffer.remaining() + " remaining bytes."  +
                    " Decoding a byte buffer into a character buffer should always "
                            + "result in an underflow result because the character buffer "
                            + "should always be big enough." );
        }
        intermediateOutputBuffer.limit( intermediateOutputBuffer.position() );
        intermediateOutputBuffer.position( 0 );
        
        final List< String > strings = buildMessagesFromCharBuffer( intermediateOutputBuffer );
        final List< JsonNetworkMessage > messages = new ArrayList<>( strings.size() );
        for ( final String str : strings )
        {
            messages.add( new JsonNetworkMessage( str ) );
        }
        return messages;
    }
    

    /**
     * Assumes that any m_byteBuffer.remaining() characters are to be copied to
     * the beginning of the next buffer so partial multibyte characters can be
     * decoded.
     */
    ByteBuffer buildBufferToDecode( final ByteBuf in )
    {
        final int newMinimumCapacity = in.readableBytes() + MAX_BYTES_FOR_CODE_POINT - 1;
        if ( m_byteBuffer == null )
        {
            m_byteBuffer = ByteBuffer.allocate( newMinimumCapacity );
        }
        else if ( newMinimumCapacity > m_byteBuffer.capacity() )
        {
            final ByteBuffer newBuffer = ByteBuffer.allocate( newMinimumCapacity );
            if ( m_byteBuffer.hasRemaining() )
            {
                newBuffer.put( m_byteBuffer );
            }
            m_byteBuffer = newBuffer;
        }
        else if ( m_byteBuffer.hasRemaining() )
        {
            final byte[] startOfNextCharacter = new byte[m_byteBuffer.remaining()];
            m_byteBuffer.get( startOfNextCharacter );
            m_byteBuffer.position( 0 );
            m_byteBuffer.put( startOfNextCharacter );
        }
        else
        {
            m_byteBuffer.position( 0 );
        }

        m_byteBuffer.limit( m_byteBuffer.position() + in.readableBytes() );
        in.readBytes( m_byteBuffer );
        m_byteBuffer.position( 0 );
        
        return m_byteBuffer;
    }
    
    
    CharBuffer buildCharBuffer( final int capacity )
    {
        if ( m_charBuffer == null || m_charBuffer.capacity() < capacity )
        {
            m_charBuffer = CharBuffer.allocate( capacity );
        }
        m_charBuffer.limit( m_charBuffer.capacity() );
        m_charBuffer.position( 0 );
        return m_charBuffer;
    }
    
    
    List< String > buildMessagesFromCharBuffer( final CharBuffer buffer )
    {
        final List< String > messages = new ArrayList<>();
        
        int start = 0;
        for ( int i = 0; i < buffer.limit(); i++ )
        {
            if ( buffer.charAt( i ) == JsonNetworkMessage.END )
            {
                if ( i > start )
                {
                    m_currentMessage.append( buffer, start, i );
                }
                start = i + 1;
                messages.add( m_currentMessage.toString() );
                m_currentMessage = new StringBuilder();
            }
        }
        if ( start < buffer.limit() )
        {
            m_currentMessage.append( buffer, start, buffer.limit() );
        }
        
        return messages;
    }
    
    
    private StringBuilder m_currentMessage = new StringBuilder();
    private ByteBuffer m_byteBuffer;
    private CharBuffer m_charBuffer;
    
    private final CharsetDecoder m_decoder;
    private static final int MAX_BYTES_FOR_CODE_POINT = 4;
}
