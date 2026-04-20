/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.tcpip.message.json;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class DecoderForChannel_Test 
{
    @Test
    public void testDecodeCanBuildMultipleMessagesFromBuffersSplitAcrossCharacters()
            throws UnsupportedEncodingException
    {
        final Logger log = Logger.getLogger( DecoderForChannel_Test.class );
        log.info( "user.dir: " + System.getProperty( "user.dir" ) );
        log.info( "java.runtime.version: " +
                           System.getProperty( "java.runtime.version" ) );
        log.info( "java.library.path: " +
                           System.getProperty( "java.library.path" ) );
        log.info( "java.specification.version: " +
                           System.getProperty( "java.specification.version" ) );
        log.info( "java.home: " + System.getProperty( "java.home" ) );
        log.info( "java.version: " + System.getProperty( "java.version" ) );
        log.info( "sun.io.unicode.encoding: " + System.getProperty( "sun.io.unicode.encoding" ) );
        log.info( "file.encoding.pkg: " + System.getProperty( "file.encoding.pkg" ) );
        log.info( "sun.jnu.encoding: " + System.getProperty( "sun.jnu.encoding" ) );
        log.info( "file.encoding: " + System.getProperty( "file.encoding" ) );
        
        TestUtil.assertJvmEncodingIsUtf8();

        final byte[] inputBytes = (
                "abξ" + JsonNetworkMessage.END
                + JsonNetworkMessage.END
                + "heφlo" + JsonNetworkMessage.END
                + "foo" + JsonNetworkMessage.END
                + "bar" + JsonNetworkMessage.END ).getBytes( "UTF-8" );

        final DecoderForChannel decoder = new DecoderForChannel( Charset.forName( "UTF-8" ) );

        final List< JsonNetworkMessage > result1 =
                decoder.decode( Unpooled.copiedBuffer( inputBytes, 0, 3 ) );
        assertEquals(new ArrayList<Object>(), result1, "Shoulda returned an empty list.");

        final List< JsonNetworkMessage > result2 =
                decoder.decode( Unpooled.copiedBuffer( inputBytes, 3, 6 ) );
        final Object expected1 = CollectionFactory.toList( "abξ", "" );
        assertEquals(expected1, extractStrings( result2 ), "Shoulda returned a list with two string elements.");

        final List< JsonNetworkMessage > result3 =
                decoder.decode( Unpooled.copiedBuffer( inputBytes, 9, inputBytes.length - 9 ) );
        final Object expected = CollectionFactory.toList( "heφlo", "foo", "bar" );
        assertEquals(expected, extractStrings( result3 ), "Shoulda returned a list with three string elements.");
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

    
    @Test
    public void testBuildBufferToDecodeCarriesUpToThreeBytesFromPreviousReadAndCanReallocateBuffer()
    {
        TestUtil.assertJvmEncodingIsUtf8();

        final DecoderForChannel decoder = new DecoderForChannel( Charset.forName( "UTF-8" ) );
        runAndVerifyBuildBufferToDecode(
                decoder,
                new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                10,
                13 );
        runAndVerifyBuildBufferToDecode(
                decoder,
                new byte[] { 10, 11, 12, 13, 14 },
                new byte[] { 10, 11 },
                5,
                13 );
        runAndVerifyBuildBufferToDecode(
                decoder,
                new byte[] { 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 },
                new byte[] { 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 },
                13,
                13 );
        runAndVerifyBuildBufferToDecode(
                decoder,
                new byte[] { 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39 },
                new byte[] { 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39 },
                16,
                18 );
        runAndVerifyBuildBufferToDecode(
                decoder,
                new byte[] { 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55 },
                new byte[] { 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55 },
                16,
                19 );
    }


    private static void runAndVerifyBuildBufferToDecode(
            final DecoderForChannel decoder,
            final byte[] inputBytes,
            final byte[] expectedBytes,
            final int expectedLimit,
            final int expectedCapacity )
    {
        final ByteBuf inputBuffer = Unpooled.copiedBuffer( inputBytes );
        final ByteBuffer result = decoder.buildBufferToDecode( inputBuffer );
        final Object expected = inputBuffer.capacity();
        assertEquals(expected, inputBuffer.readerIndex(), "Shoulda read all of the bytes in the buffer.");
        assertEquals(0,  result.position(), "Shoulda been positioned at the start.");
        assertEquals(expectedLimit,  result.limit(), "Shoulda had the expected limit.");
        assertEquals(expectedCapacity,  result.capacity(), "Shoulda had the expected capacity.");
        final byte[] resultBytes = new byte[expectedBytes.length];
        result.get( resultBytes );
        assertArrayEquals(
                expectedBytes, resultBytes,
                "Shoulda copied the expected bytes."  );
    }


    @Test
    public void testBuildMessagesFromEmptyCharBufferReturnsEmptyList()
    {
        TestUtil.assertJvmEncodingIsUtf8();

        final DecoderForChannel decoder = new DecoderForChannel( Charset.forName( "UTF-8" ) );
        final CharBuffer buffer = CharBuffer.allocate( 50 );
        buffer.limit(0);
        final List< String > result = decoder.buildMessagesFromCharBuffer( buffer );
        assertEquals(new ArrayList<Object>(), result, "Shoulda returned an empty list.");
    }
    
    
    @Test
    public void testBuildMessagesFromCharBufferWithOneResultReturnsOneResultAndHasEmptyStringBuilder()
    {
        TestUtil.assertJvmEncodingIsUtf8();

        final DecoderForChannel decoder = new DecoderForChannel( Charset.forName( "UTF-8" ) );
        final List< String > result = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "abξ" + JsonNetworkMessage.END ) );
        assertEquals(CollectionFactory.toList( "abξ" ), result, "Shoulda returned a list with the single string element.");
    }
    
    
    @Test
    public void testBuildMessagesFromCharBufferWithTwoResultsReturnsTwoResultsAndHasEmptyStringBuilder()
    {
        TestUtil.assertJvmEncodingIsUtf8();

        final DecoderForChannel decoder = new DecoderForChannel( Charset.forName( "UTF-8" ) );
        final List< String > result = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "abξ" + JsonNetworkMessage.END + "heφlo" + JsonNetworkMessage.END ) );
        final Object expected = CollectionFactory.toList( "abξ", "heφlo" );
        assertEquals(expected, result, "Shoulda returned a list with the two string elements.");
    }
    
    
    @Test
    public void testBuildMessagesFromCharBufferWithMessagesSpanningBuffers()
    {
        TestUtil.assertJvmEncodingIsUtf8();

        final DecoderForChannel decoder = new DecoderForChannel( Charset.forName( "UTF-8" ) );
        final List< String > result1 = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "abξ" + JsonNetworkMessage.END + JsonNetworkMessage.END + "he" ) );
        final List< String > result2 = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "φlo" + JsonNetworkMessage.END ) );
        final List< String > result3 = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "foo" ) );
        final List< String > result4 = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                JsonNetworkMessage.END + "bar" + JsonNetworkMessage.END + "b" ) );
        final List< String > result5 = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "a" ) );
        final List< String > result6 = decoder.buildMessagesFromCharBuffer( buildCharBufferFromString(
                "az" + JsonNetworkMessage.END ) );
        final Object expected3 = CollectionFactory.toList( "abξ", "" );
        assertEquals(expected3, result1, "Shoulda returned a list with two string elements.");
        final Object expected2 = CollectionFactory.toList( "heφlo" );
        assertEquals(expected2, result2, "Shoulda returned a list with one string element.");
        assertEquals(new ArrayList<Object>(), result3, "Shoulda returned an empty list.");
        final Object expected1 = CollectionFactory.toList( "foo", "bar" );
        assertEquals(expected1, result4, "Shoulda returned a list with two string elements.");
        assertEquals(new ArrayList<Object>(), result5, "Shoulda returned an empty list.");
        final Object expected = CollectionFactory.toList( "baaz" );
        assertEquals(expected, result6, "Shoulda returned a list with one string element.");
    }
    
    
    private static CharBuffer buildCharBufferFromString( final String input )
    {
        final CharBuffer buffer = CharBuffer.allocate( 50 );
        buffer.append( input );
        buffer.position( 0 );
        buffer.limit( input.length() );
        return buffer;
    }
}
