/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.aws;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class S3Utils_Test 
{
    @Test
    public void testBuildObjectPropertiesFromAmzCustomMetadataHeaders()
    {
        final List< S3ObjectProperty > properties =
                S3Utils.buildObjectPropertiesFromAmzCustomMetadataHeaders(
                        buildHttpRequestWithHeaders( buildTransmittedHeaders() ) );
        
        final Map< String, String > actualHeaders = new HashMap<>();
        for ( final S3ObjectProperty property : properties )
        {
            actualHeaders.put( property.getKey(), property.getValue() );
        }

        assertEquals(
                buildMetadataHeaders(),
                actualHeaders,
                "Shoulda returned a mapping with just the metadata headers."
                 );
    }
    
    
    @Test
    public void testGetMultiPartUploadETagReturnsCorrectETagWhenSingleSourceChecksum()
            throws DecoderException
    {
        final List< String > partETags = new ArrayList<>();
        partETags.add( Base64.encodeBase64String(
                Hex.decodeHex( "a54357aff0632cce46d942af68356b38".toCharArray() ) ) );
        final String objectETag = "a54357aff0632cce46d942af68356b38";
        assertEquals(objectETag, S3Utils.getObjectETag( partETags ), "Etag generated shoulda been correct.");
    }
    
    
    @Test
    public void testGetMultiPartUploadETagReturnsCorrectETagWhenMultipleSourceChecksums()
            throws DecoderException
    {
        final List< String > partETags = new ArrayList<>();
        partETags.add( Base64.encodeBase64String( 
                Hex.decodeHex( "a54357aff0632cce46d942af68356b38".toCharArray() ) ) );
        partETags.add( Base64.encodeBase64String( 
                Hex.decodeHex( "6bcf86bed8807b8e78f0fc6e0a53079d".toCharArray() ) ) );
        partETags.add( Base64.encodeBase64String( 
                Hex.decodeHex( "702242d3703818ddefe6bf7da2bed757".toCharArray() ) ) );
        final String objectETag = "fcceefe300ab53d2479300737543c9e0-3";
        assertEquals(objectETag, S3Utils.getObjectETag( partETags ), "Etag generated shoulda been correct.");
    }


    private Map< String, String > buildMetadataHeaders()
    {
        final Map< String, String > expectedHeaders = new HashMap<>();
        expectedHeaders.put(
                "x-amz-meta-this-is-my-metadata",
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit." );
        expectedHeaders.put(
                "x-amz-meta-this-is-my-other-metadata",
                "Donec ac mauris ullamcorper lorem malesuada feugiat." );
        return expectedHeaders;
    }


    private static Map< String, String > buildTransmittedHeaders()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( "Content-Length", "12345" );
        headers.put( "Content-Type", "text/html" );
        headers.put(
                "x-amz-meta-this-is-my-metadata",
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit." );
        headers.put(
                "x-amz-meta-this-is-my-other-metadata",
                "Donec ac mauris ullamcorper lorem malesuada feugiat." );
        headers.put(
                "x-amz-content-sha256",
                "4f679d9c55f17b7b193f274428b11e69002dccbb95d626c8a0632c7b11723933" );
        return headers;
    }


    private static HttpRequest buildHttpRequestWithHeaders( final Map< String, String > headers )
    {
        final Method getHeadersMethod;
        final Method getHeaderMethod;
        try
        {
            getHeadersMethod = HttpRequest.class.getMethod( "getHeaders" );
            getHeaderMethod = HttpRequest.class.getMethod( "getHeader", String.class );
        }
        catch ( final NoSuchMethodException ex )
        {
            throw new RuntimeException( ex );
        }
        final MockInvocationHandler handler = MockInvocationHandler.forMethod(
                getHeadersMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return headers;
                    }
                },
                null );
        return InterfaceProxyFactory.getProxy( HttpRequest.class, 
                MockInvocationHandler.forMethod(
                        getHeaderMethod,
                        new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method,
                                    final Object[] args ) throws Throwable
                            {
                                return headers.get( args[0] );
                            }
                        },
                        handler ) );
    }
}
