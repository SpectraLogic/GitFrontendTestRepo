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
import java.util.Map;
import java.util.UUID;



import com.spectralogic.s3.server.request.RequestParameterValueImpl;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.api.RequestParameterValue;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.request.rest.RestRequest;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RestfulCanHandleRequestDeterminer_Test
{
    @Test
    public void testHandlesReturnsFalseWhenNotValidRestRequest()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestDomainType.JOB );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        new HashMap<RequestParameterType, RequestParameterValue>(),
                        mockRestRequest( false, RestActionType.CREATE, RestDomainType.JOB ) ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenValidRestRequest()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestDomainType.JOB );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        new HashMap<RequestParameterType, RequestParameterValue>(),
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.JOB ) ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenRestActionMismatch()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestDomainType.JOB );
        assertEquals("RESTful action required: CREATE", determiner.getFailureToHandle( mockDs3Request(
                        new HashMap<RequestParameterType, RequestParameterValue>(),
                        mockRestRequest( true, RestActionType.DELETE, RestDomainType.JOB ) ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenRestDomainMismatch()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestDomainType.JOB );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        new HashMap<RequestParameterType, RequestParameterValue>(),
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.BUCKET ) ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenOperationRequiredButNotProvided()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestOperationType.ALLOCATE,
                RestDomainType.JOB );
        assertEquals("", determiner.getFailureToHandle( mockDs3Request(
                        new HashMap<RequestParameterType, RequestParameterValue>(),
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.BUCKET ) ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenOperationRequiredAndProvided()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestOperationType.ALLOCATE,
                RestDomainType.JOB );
        final Map< RequestParameterType, RequestParameterValue > requestParameters = new HashMap<>();
        requestParameters.put( RequestParameterType.OPERATION, new RequestParameterValueImpl( 
                InterfaceProxyFactory.getProxy( DS3Request.class, null ),
                RestOperationType.class, 
                RestOperationType.ALLOCATE.toString() ) );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        requestParameters,
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.JOB ) ) ), "Shoulda handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenOperationRequiredButMismatched()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestOperationType.ALLOCATE,
                RestDomainType.JOB );
        final Map< RequestParameterType, RequestParameterValue > requestParameters = new HashMap<>();
        requestParameters.put( RequestParameterType.OPERATION, new RequestParameterValueImpl(
                InterfaceProxyFactory.getProxy( DS3Request.class, null ),
                RestOperationType.class,
                RestOperationType.CANCEL_FORMAT.toString() ) );
        assertEquals("Required OPERATION query parameter value: ALLOCATE", determiner.getFailureToHandle( mockDs3Request(
                        requestParameters,
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.JOB ) ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsFalseWhenParameterRequiredButNotProvided()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestOperationType.ALLOCATE,
                RestDomainType.JOB );
        determiner.getQueryStringRequirement().registerRequiredRequestParameters( RequestParameterType.JOB );
        final Map< RequestParameterType, RequestParameterValue > requestParameters = new HashMap<>();
        requestParameters.put( RequestParameterType.OPERATION, new RequestParameterValueImpl( 
                InterfaceProxyFactory.getProxy( DS3Request.class, null ),
                RestOperationType.class,
                RestOperationType.ALLOCATE.toString() ) );
        assertEquals("Required parameter missing: job", determiner.getFailureToHandle( mockDs3Request(
                        requestParameters,
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.JOB ) ) ), "Should notta handled request.");
    }
    
    
    @Test
    public void testHandlesReturnsTrueWhenParameterRequiredAndProvided()
    {
        final CanHandleRequestDeterminer determiner = new RestfulCanHandleRequestDeterminer(
                RestActionType.CREATE,
                RestOperationType.ALLOCATE,
                RestDomainType.JOB );
        determiner.getQueryStringRequirement().registerRequiredRequestParameters( RequestParameterType.JOB );
        final Map< RequestParameterType, RequestParameterValue > requestParameters = new HashMap<>();
        requestParameters.put(
                RequestParameterType.OPERATION, 
                new RequestParameterValueImpl( 
                        InterfaceProxyFactory.getProxy( DS3Request.class, null ),
                        RestOperationType.class, 
                        RestOperationType.ALLOCATE.toString() ) );
        requestParameters.put( 
                RequestParameterType.JOB,
                new RequestParameterValueImpl( 
                        InterfaceProxyFactory.getProxy( DS3Request.class, null ),
                        UUID.class, 
                        UUID.randomUUID().toString() ) );
        assertEquals(null, determiner.getFailureToHandle( mockDs3Request(
                        requestParameters,
                        mockRestRequest( true, RestActionType.CREATE, RestDomainType.JOB ) ) ), "Shoulda handled request.");
    }

    
    private static DS3Request mockDs3Request(
            final Map< RequestParameterType, RequestParameterValue > requestParameters,
            final RestRequest restRequest )
    {
        final Method getRestRequestMethod;
        final Method getRequestParametersMethod;
        final Method getRequestParameterMethod;
        final Method getBeanPropertyValueMapFromRequestParametersMethod;
        try
        {
            getRestRequestMethod = DS3Request.class.getMethod( "getRestRequest" );
            getRequestParametersMethod = DS3Request.class.getMethod( "getRequestParameters" );
            getRequestParameterMethod =
                    DS3Request.class.getMethod( "getRequestParameter", RequestParameterType.class );
            getBeanPropertyValueMapFromRequestParametersMethod =
                    DS3Request.class.getMethod( "getBeanPropertyValueMapFromRequestParameters" );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        InvocationHandler invocationHandler = MockInvocationHandler.forMethod(
                getRestRequestMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return restRequest;
                    }
                },
                null );
        invocationHandler = MockInvocationHandler.forMethod(
                getRequestParametersMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return requestParameters.keySet();
                    }
                },
                invocationHandler );
        invocationHandler = MockInvocationHandler.forMethod(
                getRequestParameterMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return requestParameters.get( args[0] );
                    }
                },
                invocationHandler );
        invocationHandler = MockInvocationHandler.forMethod(
                getBeanPropertyValueMapFromRequestParametersMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return new HashMap< String, String >();
                    }
                },
                invocationHandler );
        return InterfaceProxyFactory.getProxy( DS3Request.class, invocationHandler );
    }


    private static RestRequest mockRestRequest(
            final boolean isValidRestRequest,
            final RestActionType restAction,
            final RestDomainType restDomain )
    {
        final Method isValidRestRequestMethod;
        final Method getActionMethod;
        final Method getDomainMethod;
        try
        {
            isValidRestRequestMethod = RestRequest.class.getMethod( "isValidRestRequest" );
            getActionMethod = RestRequest.class.getMethod( "getAction" );
            getDomainMethod = RestRequest.class.getMethod( "getDomain" );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        InvocationHandler invocationHandler = MockInvocationHandler.forMethod(
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
        invocationHandler = MockInvocationHandler.forMethod(
                getActionMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return restAction;
                    }
                },
                invocationHandler );
        invocationHandler = MockInvocationHandler.forMethod(
                getDomainMethod,
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return restDomain;
                    }
                },
                invocationHandler );
        return InterfaceProxyFactory.getProxy( RestRequest.class, invocationHandler );
    }
}
