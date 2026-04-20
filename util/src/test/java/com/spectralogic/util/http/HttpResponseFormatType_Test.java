/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.http;

import java.lang.reflect.InvocationHandler;

import javax.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public class HttpResponseFormatType_Test 
{
    @Test
    public void testValueOfReturnsJsonFromUrlInHttpServletRequest()
    {
        final String url = "http://localhost/my/thing.json";
        final HttpServletRequest request = buildMockHttpServletRequest( url, null );
        assertEquals(HttpResponseFormatType.JSON, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned JSON when the url ended with '.json'.");
    }
    
    
    @Test
    public void testValueOfReturnsXmlFromUrlInHttpServletRequest()
    {
        final String url = "http://localhost/my/thing.xml";
        final HttpServletRequest request = buildMockHttpServletRequest( url, null );
        assertEquals(HttpResponseFormatType.XML, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned XML when the url ended with '.xml'.");
    }
    
    
    @Test
    public void testValueOfReturnsJsonFromContentTypeInHttpServletResponse()
    {
        final String url = "http://localhost/my/thing.html";
        final String contentType = "application/json";
        final HttpServletRequest request = buildMockHttpServletRequest( url, contentType );
        assertEquals(HttpResponseFormatType.JSON, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned JSON when the Content-Type header was set to 'application/json'.");
    }
    
    
    @Test
    public void testValueOfReturnsXmlFromContentTypeInHttpServletResponse()
    {
        final String url = "http://localhost/my/thing.html";
        final String contentType = "application/xml";
        final HttpServletRequest request = buildMockHttpServletRequest( url, contentType );
        assertEquals(HttpResponseFormatType.XML, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned XML when the Content-Type header was set to 'application/xml'.");
    }
    
    
    @Test
    public void testValueOfUsesPathInsteadOfContentTypeWhenHttpServletResponseSpecifiesBoth()
    {
        final String url = "http://localhost/my/thing.json";
        final String contentType = "application/xml";
        final HttpServletRequest request = buildMockHttpServletRequest( url, contentType );
        assertEquals(HttpResponseFormatType.JSON, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned JSON when the Content-Type header was set"
                    + " to 'application/xml' but the path ended with '.json'.");
    }
    
    
    @Test
    public void testValueOfReturnsDefaultFromRequestWithNoContentTypeAndNoPathIndication()
    {
        final String url = "http://localhost/my/thing.html";
        final HttpServletRequest request = buildMockHttpServletRequest( url, null );
        assertEquals(HttpResponseFormatType.DEFAULT, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned DEFAULT when there wasn't any info on what format to use.");
    }
    
    
    @Test
    public void testValueOfReturnsDefaultFromRequestWithUnidentifiedContentTypeAndNoPathIndication()
    {
        final String url = "http://localhost/my/thing.html";
        final String contentType = "text/html";
        final HttpServletRequest request = buildMockHttpServletRequest( url, contentType );
        assertEquals(HttpResponseFormatType.DEFAULT, HttpResponseFormatType.valueOf( request.getPathInfo(), request.getContentType() ), "Shoulda returned DEFAULT when there wasn't any info on what format to use.");
    }
    
    
    private HttpServletRequest buildMockHttpServletRequest( final String pathInfo, final String contentType )
    {
        InvocationHandler ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getPathInfo" ),
                new ConstantResponseInvocationHandler( pathInfo ),
                null );
        ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( HttpServletRequest.class, "getContentType" ),
                new ConstantResponseInvocationHandler( contentType ),
                ih );
        return InterfaceProxyFactory.getProxy( HttpServletRequest.class, ih );
    }
}
