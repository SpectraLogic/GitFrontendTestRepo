/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;


import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestRequest;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NonRestfulCanHandleRequestDeterminer_Test 
{
    @Test
    public void testHandlesReturnsFalseWhenRequestTypeMismatch()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.GET,
                BucketRequirement.NOT_ALLOWED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                    false,
                    RequestType.POST,
                    null,
                    null ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenRequestParameterRequiredAndNoneProvided()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.NOT_ALLOWED,
                S3ObjectRequirement.NOT_ALLOWED );
        determiner.getQueryStringRequirement().registerRequiredRequestParameters( RequestParameterType.JOB );
        assertEquals("Query Parameters Required: [job], Optional: []", determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        null,
                        null ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenValidRestRequest()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.NOT_ALLOWED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        true,
                        RequestType.POST,
                        null,
                        null ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenNoBucketNoObjectAndNotAllowed()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.NOT_ALLOWED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        null,
                        null ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenNoBucketNoObjectAndOptional()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.OPTIONAL,
                S3ObjectRequirement.OPTIONAL );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        null,
                        null ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenNoBucketNoObjectAndBucketRequired()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        null,
                        null ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenBucketButNoObjectAndBucketRequired()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        "bucket_name",
                        null ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenBucketProvidedAndBucketRequiredAndObjectNotAllowed()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        "bucket_name",
                        null ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenBucketAndObjectProvidedAndBucketRequiredAndObjectNotAllowed()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        "bucket_name",
                        "object_name" ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenBucketAndObjectProvidedAndBucketRequiredAndObjectOptional()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.OPTIONAL );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        "bucket_name",
                        "object_name" ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenBucketProvidedAndBucketAndObjectRequired()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.REQUIRED );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        "bucket_name",
                        null ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenBucketAndObjectProvidedAndBucketAndObjectRequired()
    {
        final CanHandleRequestDeterminer determiner = new NonRestfulCanHandleRequestDeterminer(
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.REQUIRED );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        false,
                        RequestType.POST,
                        "bucket_name",
                        "object_name" ) ), "Shoulda handled request.");
    }

    
    private static DS3Request mockDs3Request(
            final boolean isValidRestRequest,
            final RequestType requestType,
            final String bucketName,
            final String objectName )
    {
        final Method getRestRequestMethod;
        final Method getRequestParametersMethod;
        final Method getBeanPropertyValueMapFromRequestParametersMethod;
        final Method getHttpRequest;
        final Method getBucketNameMethod;
        final Method getObjectNameMethod;
        try
        {
            getRestRequestMethod = DS3Request.class.getMethod( "getRestRequest" );
            getRequestParametersMethod = DS3Request.class.getMethod( "getRequestParameters" );
            getBeanPropertyValueMapFromRequestParametersMethod =
                    DS3Request.class.getMethod( "getBeanPropertyValueMapFromRequestParameters" );
            getHttpRequest = DS3Request.class.getMethod( "getHttpRequest" );
            getBucketNameMethod = DS3Request.class.getMethod( "getBucketName" );
            getObjectNameMethod = DS3Request.class.getMethod( "getObjectName" );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        InvocationHandler invocationHandler = MockInvocationHandler.forMethod(
                getRestRequestMethod,
                new ConstantResponseInvocationHandler( mockRestRequest( isValidRestRequest ) ),
                null );
        invocationHandler = MockInvocationHandler.forMethod(
                getRequestParametersMethod,
                new ConstantResponseInvocationHandler( new HashSet< RequestParameterType >() ),
                invocationHandler );
        invocationHandler = MockInvocationHandler.forMethod(
                getBeanPropertyValueMapFromRequestParametersMethod,
                new ConstantResponseInvocationHandler( new HashMap< String, String >() ),
                invocationHandler );
        final HttpRequest mockHttpRequest = InterfaceProxyFactory.getProxy( 
                HttpRequest.class,
                new ConstantResponseInvocationHandler( requestType ) );
        invocationHandler = MockInvocationHandler.forMethod(
                getHttpRequest,
                new ConstantResponseInvocationHandler( mockHttpRequest ),
                invocationHandler );
        invocationHandler = MockInvocationHandler.forMethod(
                getBucketNameMethod,
                new ConstantResponseInvocationHandler( bucketName ),
                invocationHandler );
        invocationHandler = MockInvocationHandler.forMethod(
                getObjectNameMethod,
                new ConstantResponseInvocationHandler( objectName ),
                invocationHandler );
        return InterfaceProxyFactory.getProxy( DS3Request.class, invocationHandler );
    }


    private static RestRequest mockRestRequest( final boolean isValidRestRequest )
    {
        final Method isValidRestRequestMethod;
        try
        {
            isValidRestRequestMethod = RestRequest.class.getMethod( "isValidRestRequest" );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        final InvocationHandler invocationHandler = MockInvocationHandler.forMethod(
                isValidRestRequestMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return Boolean.valueOf( isValidRestRequest );
                    }
                },
                null );
        return InterfaceProxyFactory.getProxy( RestRequest.class, invocationHandler );
    }
}
