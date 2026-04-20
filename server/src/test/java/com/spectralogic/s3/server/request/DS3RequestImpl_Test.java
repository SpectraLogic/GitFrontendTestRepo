package com.spectralogic.s3.server.request;


import java.io.PrintStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import com.spectralogic.s3.common.testfrmwrk.MockHttpServletRequest;
import com.spectralogic.s3.server.builder.S3RequestParserBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DS3RequestImpl_Test 
{

    @Test
    public void testGetServiceNameNoHeaders()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder(null)
                .withType(RequestType.GET)
                .build();

        final Object expected1 = s3Parser.getHttpRequest().getType();
        assertEquals(expected1, RequestType.GET, "Shoulda parsed request correctly.");
        assertNull(
                s3Parser.getBucketName(),
                "Shoulda parsed request correctly.");
        final Object expected = s3Parser.getRequestPath();
        assertEquals(expected, "", "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testGetServiceNameWithHostHeader()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder(null)
                .withType(RequestType.GET)
                .withHeader("host", "127.0.0.1")
                .build();

        assertNull(
                s3Parser.getBucketName(),
                "Shoulda parsed request correctly."
                );
    }

    
    @Test
    public void testPutNewBucket()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder(null)
                .withType(RequestType.PUT)
                .withHeader("bucket-name", "test")
                .build();

        final Object expected1 = s3Parser.getHttpRequest().getType();
        assertEquals(expected1, RequestType.PUT, "Shoulda parsed request correctly.");
        final Object expected = s3Parser.getBucketName();
        assertEquals(expected, "test", "Shoulda parsed request correctly.");
    }


    @Test
    public void testGetRequestId()
    {
        final long requestId = DS3RequestImpl.getNextRequestId();

        final DS3Request s3Parser = new S3RequestParserBuilder(null)
                .withType(RequestType.GET)
                .build();

        final Object expected = s3Parser.getRequestId();
        assertEquals(expected, requestId + 1, "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testGetObjectList()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder("/test")
                .withType(RequestType.GET)
                .build();

        final Object expected1 = s3Parser.getBucketName();
        assertEquals(expected1, "test", "Shoulda parsed request correctly.");
        assertNull(
                s3Parser.getObjectName(),
                "Shoulda parsed request correctly."
               );
        final Object expected = s3Parser.getRequestPath();
        assertEquals(expected, "/test", "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testGetObject()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder("/test/item")
                .withType(RequestType.GET)
                .build();

        final Object expected1 = s3Parser.getBucketName();
        assertEquals(expected1, "test", "Shoulda parsed request correctly.");
        final Object expected = s3Parser.getObjectName();
        assertEquals(expected, "item", "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testUrlWithoutStartingSlash()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder("test")
                .withType(RequestType.GET)
                .build();

        final Object expected = s3Parser.getBucketName();
        assertEquals(expected, "test", "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testUrlWithEndingSlash()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder("test/")
                .withType(RequestType.GET)
                .build();

        final Object expected = s3Parser.getBucketName();
        assertEquals(expected, "test", "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testAuthParsing()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder("test/")
                .withHeader("Authorization", "AWS myid:signature")
                .withType(RequestType.GET)
                .build();

        final Object expected1 = s3Parser.getBucketName();
        assertEquals(expected1, "test", "Shoulda parsed request correctly.");
        final Object expected = s3Parser.getAuthorization().getId();
        assertEquals(expected, "myid", "Shoulda parsed request correctly.");
    }

    
    @Test
    public void testMissingHeader()
    {
        final DS3Request s3Parser = new S3RequestParserBuilder("test/")
                .withType(RequestType.GET)
                .build();

        assertNull(
                s3Parser.getAuthorization().getId(),
                "Shoulda parsed request correctly."
                );
    }
    
    
    @Test
    public void testPerformance()
    {
        final HttpServletRequest request = 
                new MockHttpServletRequest( RequestType.GET, "" ).generate();
        final HttpServletResponse response =
                InterfaceProxyFactory.getProxy( HttpServletResponse.class, null );
        
        int count = 0;
        final Duration duration = new Duration();
        while ( duration.getElapsedMillis() < 10 )
        {
            new DS3RequestImpl( request, response );
            ++count;
        }
        
        final PrintStream out = System.out;
        out.println( 
            "Can instantiate " + (count * 100) + " " + DS3RequestImpl.class.getSimpleName() + "s / second." );
    }
}