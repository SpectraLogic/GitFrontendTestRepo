/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request;

import java.lang.reflect.Method;
import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterValue;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class RequestParameterValueImpl_Test 
{

    @Test
    public void testConstructorNullRequestNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new RequestParameterValueImpl( null, int.class, "12" );
            }
        } );
    }
    
    
    @Test
    public void testConstructorNullTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new RequestParameterValueImpl( getRequest( RequestType.POST ), null, "12" );
            }
        } );
    }
    

    @Test
    public void testConstructorNullValueNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new RequestParameterValueImpl( getRequest( RequestType.POST ), int.class, null );
            }
        } );
    }
    

    @Test
    public void testConstructorUnsupportedTypeNotAllowed()
    {
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test()
            {
                new RequestParameterValueImpl( getRequest( RequestType.POST ), Integer.class, "12" );
            }
        } );
    }
    

    @Test
    public void testPrimitiveIntHandledCorrectly()
    {
        final RequestParameterValue value = 
                new RequestParameterValueImpl( getRequest( RequestType.POST ), int.class, "12" );
        assertEquals(
                12,
                value.getInt(),
                "Shoulda parsed input correctly."
                 );

        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        value.getLong();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getBean( new MockBeansServiceManager().getRetriever( User.class ) );
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getUuid();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getEnum( S3ObjectType.class );
                    }
                } );
    }
    

    @Test
    public void testPrimitiveLongHandledCorrectly()
    {
        final RequestParameterValue value = 
                new RequestParameterValueImpl( getRequest( RequestType.POST ), long.class, "12" );
        assertEquals(
                12,
                value.getLong(),
                "Shoulda parsed input correctly."
                 );

        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        value.getInt();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        value.getBean( new MockBeansServiceManager().getRetriever( User.class ) );
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getUuid();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getEnum( S3ObjectType.class );
                    }
                } );
    }
    

    @Test
    public void testStringHandledCorrectly()
    {
        final RequestParameterValue value = 
                new RequestParameterValueImpl( getRequest( RequestType.POST ), String.class, "cat" );
        assertEquals(
                "cat",
                value.getString(),
                "Shoulda parsed input correctly."
                 );

        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getLong();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getInt();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getBean( new MockBeansServiceManager().getRetriever( User.class ) );
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getUuid();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {

                    public void test()
                    {
                        value.getEnum( S3ObjectType.class );
                    }
                } );
    }
    

    @Test
    public void testEnumValueHandledCorrectly()
    {
        final RequestParameterValue value = 
                new RequestParameterValueImpl( 
                        getRequest( RequestType.POST ), 
                        S3ObjectType.class, 
                        S3ObjectType.DATA.toString().toLowerCase() );
        assertEquals(
                S3ObjectType.DATA,
                value.getEnum( S3ObjectType.class ),
                "Shoulda parsed input correctly."
                 );

        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getInt();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getBean( new MockBeansServiceManager().getRetriever( User.class ) );
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getUuid();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getLong();
                    }
                } );
    }
    

    @Test
    public void testEnumValidatedIfNotAllConstantsAreUserSpecifiable()
    {
        final RequestParameterValue value = 
                new RequestParameterValueImpl(
                        getRequest( RequestType.POST ), 
                        BlobStoreTaskPriority.class, 
                        BlobStoreTaskPriority.CRITICAL.toString().toLowerCase() );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                GenericFailure.FORBIDDEN,
                new BlastContainer()
                {
                    public void test()
                    {
                        assertEquals(
                                BlobStoreTaskPriority.CRITICAL,
                                value.getEnum( BlobStoreTaskPriority.class ),
                                "Shoulda parsed input correctly."
                                 );
                    }
                } );
    }
    

    @Test
    public void testUuidHandlingCorrectly()
    {
        final UUID id = UUID.randomUUID();
        final RequestParameterValue value = 
                new RequestParameterValueImpl(
                        getRequest( RequestType.POST ), 
                        UUID.class, 
                        id.toString() );
        assertEquals(
                id,
                value.getUuid(),
                "Shoulda parsed input correctly."
                );
        assertEquals(
                null,
                value.getBean( new MockBeansServiceManager().getRetriever( User.class ) ),
                "Shoulda parsed input correctly."
                );

        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getInt();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getLong();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getEnum( S3ObjectType.class );
                    }
                } );
    }
    

    @Test
    public void testVoidHandledCorrectly()
    {
        final UUID id = UUID.randomUUID();
        final RequestParameterValue value = 
                new RequestParameterValueImpl( getRequest( RequestType.POST ), void.class, id.toString() );
        
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getUuid();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getBean( new MockBeansServiceManager().getRetriever( User.class ) );
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getInt();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test()
                    {
                        value.getLong();
                    }
                } );
        TestUtil.assertThrows(
                "Should notta allowed invalid method calls.",
                IllegalStateException.class,
                new BlastContainer()
                {
                public void test()
                    {
                        value.getEnum( S3ObjectType.class );
                    }
                } );
    }
    
    
    private DS3Request getRequest( final RequestType requestType )
    {
        final Method methodGetType = ReflectUtil.getMethod( HttpRequest.class, "getType" );
        final HttpRequest request = InterfaceProxyFactory.getProxy(
                HttpRequest.class, 
                MockInvocationHandler.forMethod(
                        methodGetType, 
                        new ConstantResponseInvocationHandler( requestType ), 
                        null ) );
        
        final Method methodGetHttpRequest = ReflectUtil.getMethod( DS3Request.class, "getHttpRequest" );
        return InterfaceProxyFactory.getProxy(
                DS3Request.class, 
                MockInvocationHandler.forMethod( 
                        methodGetHttpRequest, 
                        new ConstantResponseInvocationHandler( request ),
                        null ) );
    }
}
