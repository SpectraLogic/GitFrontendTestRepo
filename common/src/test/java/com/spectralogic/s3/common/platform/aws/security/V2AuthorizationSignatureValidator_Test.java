/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.aws.security;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockHttpServletRequest;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.http.ServletHttpRequest;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.marshal.DateMarshaler;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class V2AuthorizationSignatureValidator_Test 
{
    @Test
    public void testAuthorizationPassesWhenValidRequestBasic1()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + "/abc/def",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesWhenValidRequestBasic2()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def/";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + "/abc/def/",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesWhenValidRequestBasic3()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, "a.com" )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesForAmazonSpecExample1()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + url,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesForAmazonSpecExample2()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( "Content-Length", "94328" );
        headers.put( "Content-Type", "image/jpeg" );
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.PUT, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "PUT" + Platform.SLASH_N + Platform.SLASH_N
                                + "image/jpeg" + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + url,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesForAmazonSpecExample3()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        queryParameters.put( "prefix", "photos" );
        queryParameters.put( "max-keys", "50" );
        queryParameters.put( "marker", "puppy" );
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + url,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesForAmazonSpecExample4()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        queryParameters.put( "acl", null );
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/johnsmith/";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + url + "?acl",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesForAmazonSpecExample5()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/johnsmith/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.DELETE, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "DELETE" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + url,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesForAmazonSpecExample6()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        final Map< String, String > headers = new HashMap<>();
        headers.put( "x-amz-acl", "public-read" );
        headers.put( "content-type", "application/x-download" );
        headers.put( "Content-MD5", "4gJE4saaMU4BqNR0kLY+lw==" );
        headers.put( "X-Amz-Meta-ReviewedBy", "jane@johnsmith.net" );
        headers.put( "X-Amz-Meta-FileChecksum", "0x02661779" );
        headers.put( "X-Amz-Meta-ChecksumAlgorithm", "crc32" );
        headers.put( "Content-Disposition", "attachment; filename=database.dat" );
        headers.put( "Content-Encoding", "gzip" );
        headers.put( "Content-Length", "5913339" );
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/johnsmith/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.DELETE, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "DELETE" + Platform.SLASH_N
                                + "4gJE4saaMU4BqNR0kLY+lw==" + Platform.SLASH_N
                                + "application/x-download" + Platform.SLASH_N 
                                + s3Date + Platform.SLASH_N 
                                + "x-amz-acl:public-read" + Platform.SLASH_N
                                + "x-amz-meta-checksumalgorithm:crc32" + Platform.SLASH_N
                                + "x-amz-meta-filechecksum:0x02661779" + Platform.SLASH_N
                                + "x-amz-meta-reviewedby:jane@johnsmith.net" + Platform.SLASH_N
                                + url,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesWhenDateHeaderIsOverridenByAmazonDateHeader()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        final Map< String, String > headers = new HashMap<>();
        headers.put( "x-amz-acl", "public-read" );
        headers.put( "content-type", "application/x-download" );
        headers.put( "Content-MD5", "4gJE4saaMU4BqNR0kLY+lw==" );
        headers.put( "X-Amz-Meta-ReviewedBy", "jane@johnsmith.net" );
        headers.put( "X-Amz-Meta-FileChecksum", "0x02661779" );
        headers.put( "X-Amz-Meta-ChecksumAlgorithm", "crc32" );
        headers.put( "Content-Disposition", "attachment; filename=database.dat" );
        headers.put( "Content-Encoding", "gzip" );
        headers.put( "Content-Length", "5913339" );
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( "x-amz-Date", s3Date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(),
                     DateMarshaler.marshal( new Date( 100000 ) ) );
        final String url = "/johnsmith/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.DELETE, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "DELETE" + Platform.SLASH_N +
                                "4gJE4saaMU4BqNR0kLY+lw==" + Platform.SLASH_N
                                + "application/x-download" + Platform.SLASH_N 
                                + Platform.SLASH_N 
                                + "x-amz-acl:public-read" + Platform.SLASH_N
                                + "x-amz-date:" + s3Date + Platform.SLASH_N
                                + "x-amz-meta-checksumalgorithm:crc32" + Platform.SLASH_N
                                + "x-amz-meta-filechecksum:0x02661779" + Platform.SLASH_N
                                + "x-amz-meta-reviewedby:jane@johnsmith.net" + Platform.SLASH_N
                                + url,
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesWhenValidRequestAndAlternateDateFormat()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date();
        final String s3Date = date.toString();
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, url )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + "/abc/def",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesRequestWhenValidAndSlightClientClockSkewIntoThePast()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date( System.currentTimeMillis() - 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + "/abc/def",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesRequestWhenValidAndSlightClientClockSkewIntoTheFuture()
    {
        final Map< String, String > headers = new HashMap<>();
        
        final Date date = new Date( System.currentTimeMillis() + 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                + s3Date + Platform.SLASH_N
                                + "/abc/def",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenClientDoesNotSupplyDate()
    {
        final Map< String, String > headers = new HashMap<>();
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N
                                                + Platform.SLASH_N + Platform.SLASH_N
                                                + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenClientDateIsInvalid()
    {
        final Map< String, String > headers = new HashMap<>();

        final String s3Date = "invalid";
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                                + s3Date + Platform.SLASH_N
                                                + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenClientDateIsInThePastTooMuch()
    {
        final Map< String, String > headers = new HashMap<>();

        final Date date = new Date( System.currentTimeMillis() - 16 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        TestUtil.assertThrows(
                null,
                AWSFailure.REQUEST_TIME_TOO_SKEWED, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                                + s3Date + Platform.SLASH_N
                                                + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenClientDateIsInTheFutureTooMuch()
    {
        final Map< String, String > headers = new HashMap<>();

        final Date date = new Date( System.currentTimeMillis() + 16 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
            .setHeaders( headers ).generate();
        TestUtil.assertThrows(
                null,
                AWSFailure.REQUEST_TIME_TOO_SKEWED, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                                + s3Date + Platform.SLASH_N
                                                + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationPassesWhenClientDateIsInThePastBeyondAllowedSkewButWithinCustomAuthTimeout()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName(), "3600" );

        final Date date = new Date( System.currentTimeMillis() - 62 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final Map< String, String > queryParameters = new HashMap<>();
        final HttpServletRequest request = 
                new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
                .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                        + s3Date + Platform.SLASH_N
                        + S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName().toLowerCase() 
                        + ":3600" + Platform.SLASH_N
                        + "/abc/def",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationPassesWhenClientDateIsInTheFutureWithinAllowedSkewWhenCustomAuthTimeout()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName(), "3600" );

        final Date date = new Date( System.currentTimeMillis() + 2 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final Map< String, String > queryParameters = new HashMap<>();
        final HttpServletRequest request = 
                new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
                .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        new V2AuthorizationSignatureValidator(
                new ServletHttpRequest( request ), 
                "secret" ).validate( sign(
                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                        + s3Date + Platform.SLASH_N
                        + S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName().toLowerCase() 
                        + ":3600" + Platform.SLASH_N
                        + "/abc/def",
                        "secret" ) );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenClientDateIsInThePastBeyondAllowedSkewAndCustomAuthTimeout()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName(), "3600" );

        final Date date = new Date( System.currentTimeMillis() - 79 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final Map< String, String > queryParameters = new HashMap<>();
        final HttpServletRequest request = 
                new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
                .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        TestUtil.assertThrows(
                null,
                AWSFailure.REQUEST_TIME_TOO_SKEWED, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                        + s3Date + Platform.SLASH_N
                                        + S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName().toLowerCase() 
                                        + ":3600" + Platform.SLASH_N
                                        + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenClientDateIsInTheFutureBeyondAllowedSkewWhenCustomAuthTimeout()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName(), "3600" );

        final Date date = new Date( System.currentTimeMillis() + 17 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final Map< String, String > queryParameters = new HashMap<>();
        final HttpServletRequest request = 
                new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
                .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        TestUtil.assertThrows(
                null,
                AWSFailure.REQUEST_TIME_TOO_SKEWED, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                        + s3Date + Platform.SLASH_N
                                        + S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName().toLowerCase() 
                                        + ":3600" + Platform.SLASH_N
                                        + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationFailsWhenCustomAuthTimeoutAndCustomAuthTimeoutNotIncludedInSignature()
    {
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.AUTH_DATE_GRACE.getHttpHeaderName(), "3600" );

        final Date date = new Date( System.currentTimeMillis() - 7 * 60 * 1000 );
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/abc/def";
        final Map< String, String > queryParameters = new HashMap<>();
        final HttpServletRequest request = 
                new MockHttpServletRequest( RequestType.GET, getFullUrl( url ) )
                .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        TestUtil.assertThrows(
                null,
                IllegalArgumentException.class, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "GET" + Platform.SLASH_N + Platform.SLASH_N + Platform.SLASH_N
                                        + s3Date + Platform.SLASH_N
                                        + "/abc/def",
                                        "secret" ) );
                    }
                } );
    }
    
    
    @Test
    public void testAuthorizationDoesNotUseEqualsWhenSubresourceParameterIsEmpty()
    {
        final String s3Date = DateMarshaler.marshal( new Date() );

        final Map< String, String > queryParameters = new HashMap<>();
        queryParameters.put( "delete", "" );

        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        headers.put(
                S3HeaderType.CONTENT_MD5.getHttpHeaderName(),
                "59hxIrP+qtB54UV4Tq+w/w==" );

        final String url = "Ds3ClientTestPlan3";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.POST, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        
        new V2AuthorizationSignatureValidator( new ServletHttpRequest( request ), "secret" ).validate( sign(
                "POST" + Platform.SLASH_N
                    + "59hxIrP+qtB54UV4Tq+w/w==" + Platform.SLASH_N
                    + Platform.SLASH_N
                    + s3Date + Platform.SLASH_N
                    + "/Ds3ClientTestPlan3?delete",
                "secret" ) );
    }
    
    
    @Test
    public void testPerformance()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        final Map< String, String > headers = new HashMap<>();
        headers.put( "x-amz-acl", "public-read" );
        headers.put( "content-type", "application/x-download" );
        headers.put( "Content-MD5", "4gJE4saaMU4BqNR0kLY+lw==" );
        headers.put( "X-Amz-Meta-ReviewedBy", "jane@johnsmith.net" );
        headers.put( "X-Amz-Meta-FileChecksum", "0x02661779" );
        headers.put( "X-Amz-Meta-ChecksumAlgorithm", "crc32" );
        headers.put( "Content-Disposition", "attachment; filename=database.dat" );
        headers.put( "Content-Encoding", "gzip" );
        headers.put( "Content-Length", "5913339" );
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/johnsmith/photos/puppy.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.DELETE, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        
        final String signature = sign(
                "DELETE" + Platform.SLASH_N
                        + "4gJE4saaMU4BqNR0kLY+lw==" + Platform.SLASH_N
                        + "application/x-download" + Platform.SLASH_N 
                        + s3Date + Platform.SLASH_N 
                        + "x-amz-acl:public-read" + Platform.SLASH_N
                        + "x-amz-meta-checksumalgorithm:crc32" + Platform.SLASH_N
                        + "x-amz-meta-filechecksum:0x02661779" + Platform.SLASH_N
                        + "x-amz-meta-reviewedby:jane@johnsmith.net" + Platform.SLASH_N
                        + url,
                "secret" );
        
        final Duration duration = new Duration();
        int count = 0;
        while ( 100 > duration.getElapsedMillis() )
        {
            for ( int j = 0; j < 1000; ++j )
            {
                new V2AuthorizationSignatureValidator( 
                        new ServletHttpRequest( request ), "secret" ).validate( signature );
            }
            count += 1000;
        }
        final int perSecond = count * 1000 / (int)duration.getElapsedMillis();
        LOG.info( "Validating authorization signatures at " + perSecond + "/sec." );
    }
    

    @Test
    public void testAuthorizationPassesForAllChecksumTypes()
    {
        for( ChecksumType checksumType : ChecksumType.values() )
        {
            final Map< String, String > queryParameters = new HashMap<>();
            final Map< String, String > headers = new HashMap<>();
            final String someChecksum = "someChecksum==";
            headers.put( "content-type", "image/jpeg" );
            headers.put( checksumType.getHttpHeaderName(), someChecksum );
            
            final Date date = new Date();
            final String s3Date = DateMarshaler.marshal( date );
            headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
            final String url = "/photos/Spectra.jpg";
            final HttpServletRequest request = new MockHttpServletRequest( 
                    RequestType.PUT, 
                    getFullUrl( url ) )
                        .setHeaders( headers )
                        .setQueryParameters( queryParameters )
                        .generate();
            
            new V2AuthorizationSignatureValidator(
                    new ServletHttpRequest( request ), 
                    "secret" ).validate( sign(
                            "PUT" + Platform.SLASH_N
                                  + someChecksum + Platform.SLASH_N
                                  + "image/jpeg" + Platform.SLASH_N 
                                  + s3Date + Platform.SLASH_N 
                                  + url,
                                  "secret" ) );
        }
    }
    
    
    @Test
    public void testAuthorizationFailsForMultipleValidationHeaders()
    {
        final Map< String, String > queryParameters = new HashMap<>();
        final Map< String, String > headers = new HashMap<>();
        final String shaChecksum = "someShaChecksum==";
        final String md5Checksum = "someMd5Checksum==";
        headers.put( "content-type", "image/jpeg" );
        headers.put( "Content-SHA256", shaChecksum );
        headers.put( "Content-MD5", md5Checksum );
        
        final Date date = new Date();
        final String s3Date = DateMarshaler.marshal( date );
        headers.put( S3HeaderType.DATE.getHttpHeaderName(), s3Date );
        final String url = "/photos/Spectra.jpg";
        final HttpServletRequest request = new MockHttpServletRequest( RequestType.PUT, getFullUrl( url ) )
            .setHeaders( headers ).setQueryParameters( queryParameters ).generate();
        
        TestUtil.assertThrows(
                null,
                AWSFailure.MULTIPLE_CHECKSUM_HEADERS, new BlastContainer()
                {
                    public void test()
                    {
                        new V2AuthorizationSignatureValidator(
                                new ServletHttpRequest( request ), 
                                "secret" ).validate( sign(
                                        "PUT" + Platform.SLASH_N
                                              + shaChecksum + Platform.SLASH_N
                                              + "image/jpeg" + Platform.SLASH_N 
                                              + s3Date + Platform.SLASH_N 
                                              + url,
                                              "secret" ) );
                    }
                } );
    }
    
    
    private String sign( final String stringToSign, final String secretKey )
    {
        try
        {
            final byte[] secretyKeyBytes = secretKey.getBytes( "UTF8" );
            final SecretKeySpec secretKeySpec = new SecretKeySpec( secretyKeyBytes, "HmacSHA1" );
            final Mac mac = Mac.getInstance( "HmacSHA1" );
            mac.init(secretKeySpec);
            
            final byte[] data = stringToSign.getBytes( "UTF8" );
            final byte[] rawHmac = mac.doFinal( data );
            return Base64.encodeBase64String( rawHmac );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private String getFullUrl( String path )
    {
        if ( !path.startsWith( "/" ) )
        {
            path = "/" + path;
        }
        
        return "http://localhost" + path;
    }
    
    
    private final static Logger LOG = Logger.getLogger( V2AuthorizationSignatureValidator_Test.class );
}
