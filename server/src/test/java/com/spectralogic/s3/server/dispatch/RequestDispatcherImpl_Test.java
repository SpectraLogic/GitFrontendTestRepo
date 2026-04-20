/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.dispatch;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcRequestUnserviceableException;



public final class RequestDispatcherImpl_Test
{
    public void testRequestDispatcherDispatchesRequestWhenHandlerExistsThatWillAcceptRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    public void testRequestDispatcherGivesUsefulFailureWhenRestDomainTypeIsInvalid()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + "invalidme" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( "Domain &apos;INVALIDME&apos; does not exist" );
        for ( final RestDomainType domain : RestDomainType.values() )
        {
            driver.assertResponseToClientContains( domain.toString() );
        }
    }
    
    
    public void testRequestDispatcherGivesUsefulFailureWhenVeryCloseWrtQueryParameters()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER.toString() ).addParameter( "invalidparam", "hello" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( "Parameter unknown: invalidparam" );
    }
    
    
    public void testRequestDispatcherGivesUsefulFailureWhenCloseWrtQueryParameters()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER.toString() )
                .addParameter( "invalidparam", "hello" )
                .addParameter( "drink", "scotch" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( "Parameter unknown: invalidparam" );
        driver.assertResponseToClientContains( "Parameter unknown: drink" );
    }
    
    
    public void testRequestDispatcherReturnsRetryAfterHeaderInSeconds()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( buildAsynchronousWaitDataPlanner() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Blob blob =
                mockDaoDriver.getBlobFor( mockDaoDriver.createObjectStub( null, "foo", 1024L ).getId() );
        mockDaoDriver
                .createJobWithEntry( JobRequestType.PUT, blob )
                .getId();
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/foo" ).addHeader(
                        S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(), String.valueOf( blob.getLength() ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 503 );
        final Map< String, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( "Retry-After", "1234" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }
    
    
    public void testRpcResourceNotServicibleExceptionHandledGracefully()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( new InvocationHandler()
        {
            public Object invoke(
                    final Object proxy, 
                    final Method method, 
                    final Object[] args ) throws Throwable
            {
                final Constructor< ? extends Throwable > con =
                        RpcRequestUnserviceableException.class.getDeclaredConstructor(
                                String.class, Throwable.class );
                con.setAccessible( true );
                throw con.newInstance(
                        "I'm not servicible.",
                        new RuntimeException( "Really I'm not servicible." ) );
            }
        } );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Blob blob =
                mockDaoDriver.getBlobFor( mockDaoDriver.createObjectStub( null, "foo", 1024L ).getId() );
        mockDaoDriver
                .createJobWithEntry( JobRequestType.PUT, blob )
                .getId();
        mockDaoDriver.enableImplicitJobResolutionForAllJobs();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                MockDaoDriver.DEFAULT_BUCKET_NAME + "/foo" ).addHeader(
                        S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(), String.valueOf( blob.getLength() ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 503 );
    }


    private static InvocationHandler buildAsynchronousWaitDataPlanner()
    {
        try
        {
            return MockInvocationHandler.forMethod(
                   DataPlannerResource.class.getMethod( "startBlobWrite", UUID.class, UUID.class ),
                   new InvocationHandler()
                   {
                       @Override
                       public Object invoke( final Object proxy, final Method method, final Object[] args )
                               throws Throwable
                       {
                           throw new S3RestException(
                                   GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                                   "Testing the Retry-After header." )
                                           .setRetryAfter( 1234 );
                       }
                   },
                   null );
        }
        catch ( final NoSuchMethodException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
    }
}
