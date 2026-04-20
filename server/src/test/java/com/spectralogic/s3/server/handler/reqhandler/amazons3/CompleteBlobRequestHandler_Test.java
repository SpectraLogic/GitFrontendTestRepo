/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.*;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.MockInvocationHandler;
import org.junit.jupiter.api.Test;


import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompleteBlobRequestHandler_Test 
{
    @Test
    public void testBasicValidityForPuts()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                                       .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                                        .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                                                  .toString() )
                                                                                    .addParameter(
                                                                                            RequestParameterType.JOB.toString(),
                                                                                            jobId.toString() )
                                                                                    .addHeader(
                                                                                            S3HeaderType.CONTENT_MD5,
                                                                                            "0123456789abcdef" )
                                                                                    .addHeader(
                                                                                            S3HeaderType.CONTENT_LENGTH,
                                                                                            "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testCancelNotAllowedForMultiBlobGetJobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final S3Object object2 = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME + "2", -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 10 )
                .get( 0 );
        final Blob blob2 = mockDaoDriver.createBlobs( object2.getId(), 1, 10 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.GET )
                .getId();
        mockDaoDriver.createJobEntries(jobId, CollectionFactory.toSet( blob, blob2 ) );
        mockDaoDriver.createBlobCache( blob2.getId(), CacheEntryState.IN_CACHE);

        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );

        final BasicTestsInvocationHandler brcInvocationHandler = new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "blobReadCompleted" ), (proxy, method, args ) -> {
                    assertEquals((UUID) args[0],jobId );
                    assertEquals((UUID) args[1], blob2.getId() );
                    return null;
                },
                null ) );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "blobReadCompleted" ),
                brcInvocationHandler,
                null ) );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                                RequestParameterType.BLOB.toString(), blob.getId()
                                        .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );


        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testMultiBlobGetJobAllowedIfAllInCache()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final S3Object object2 = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME + "2", -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 10 )
                .get( 0 );
        final Blob blob2 = mockDaoDriver.createBlobs( object2.getId(), 1, 10 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.GET )
                .getId();
        mockDaoDriver.createJobEntries(jobId, CollectionFactory.toSet( blob, blob2 ) );
        mockDaoDriver.createBlobCache( blob.getId(), CacheEntryState.IN_CACHE);
        mockDaoDriver.createBlobCache( blob2.getId(), CacheEntryState.IN_CACHE);

        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );

        final BasicTestsInvocationHandler brcInvocationHandler = new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "blobReadCompleted" ), (proxy, method, args ) -> {
                    assertEquals(jobId, (UUID) args[0]);
                    return null;
                },
                null ) );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "blobReadCompleted" ),
                brcInvocationHandler,
                null ) );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                                RequestParameterType.BLOB.toString(), blob.getId()
                                        .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );


        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals( 0, cancelJobInvocationHandler.getJobIds().size() );

        driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                                RequestParameterType.BLOB.toString(), blob2.getId()
                                        .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals( 0, cancelJobInvocationHandler.getJobIds().size() );
    }


    @Test
    public void testForGetWhenBlobInCache()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.GET )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        mockDaoDriver.createBlobCache( blob.getId(), CacheEntryState.IN_CACHE);

        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );

        final BasicTestsInvocationHandler brcInvocationHandler = new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "blobReadCompleted" ), (proxy, method, args ) -> {
                    assertEquals(jobId, (UUID) args[0]);
                    assertEquals(blob.getId(), (UUID) args[1]);
                    return null;
                },
                null ) );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "blobReadCompleted" ),
                brcInvocationHandler,
                null ) );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                                RequestParameterType.BLOB.toString(), blob.getId()
                                        .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals( 0, cancelJobInvocationHandler.getJobIds().size() );
    }


    @Test
    public void testForGetWhenBlobAllocatedButNotInCache()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.GET )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        mockDaoDriver.createBlobCache( blob.getId(), CacheEntryState.ALLOCATED);

        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                                RequestParameterType.BLOB.toString(), blob.getId()
                                        .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertTrue( cancelJobInvocationHandler.getJobIds().contains( jobId ) );
    }


    @Test
    public void testForGetWhenBlobNotAllocated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.GET )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final CancelJobInvocationHandler cancelJobInvocationHandler =
                new CancelJobInvocationHandler( support.getDatabaseSupport() );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( JobResource.class, "cancelJob" ),
                cancelJobInvocationHandler,
                null ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                                RequestParameterType.BLOB.toString(), blob.getId()
                                        .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertTrue( cancelJobInvocationHandler.getJobIds().contains( jobId ) );
    }


    @Test
    public void testValidityWithSmallerBlobSize()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, 10 );
        final Blob blob = mockDaoDriver.createBlobsAtOffset( object.getId(), 1, 10, 1 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addParameter(
                                RequestParameterType.SIZE.toString(),
                                "1" )
                        .addHeader(
                                S3HeaderType.CONTENT_MD5,
                                "0123456789abcdef" )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testFailsWithLargerBlobSize()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, 10 );
        final Blob blob = mockDaoDriver.createBlobsAtOffset( object.getId(), 1, 10, 1 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addParameter(
                                RequestParameterType.SIZE.toString(),
                                "100" )
                        .addHeader(
                                S3HeaderType.CONTENT_MD5,
                                "0123456789abcdef" )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testValidityWithZeroBlobSize()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, 10 );
        final Blob blob = mockDaoDriver.createBlobsAtOffset( object.getId(), 1, 10, 1 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addParameter(
                                RequestParameterType.SIZE.toString(),
                                "0" )
                        .addHeader(
                                S3HeaderType.CONTENT_MD5,
                                "0123456789abcdef" )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testFailsWithNegativeBlobSize()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, 10 );
        final Blob blob = mockDaoDriver.createBlobsAtOffset( object.getId(), 1, 10, 1 )
                .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                .toString() )
                        .addParameter(
                                RequestParameterType.JOB.toString(),
                                jobId.toString() )
                        .addParameter(
                                RequestParameterType.SIZE.toString(),
                                "-1" )
                        .addHeader(
                                S3HeaderType.CONTENT_MD5,
                                "0123456789abcdef" )
                        .addHeader(
                                S3HeaderType.CONTENT_LENGTH,
                                "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testFailsWithWrongChecksumType()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                                       .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                                        .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                                                  .toString() )
                                                                                    .addParameter(
                                                                                            RequestParameterType.JOB.toString(),
                                                                                            jobId.toString() )
                
                                                                                    .addHeader( "Content-CRC",
                                                                                            "0123456789abcdef" )
                                                                                    .addHeader(
                                                                                            S3HeaderType.CONTENT_LENGTH,
                                                                                            "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testFailsWithoutChecksum()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                                       .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                                        .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getId()
                                                                  .toString() )
                                                                                    .addParameter(
                                                                                            RequestParameterType.JOB.toString(),
                                                                                            jobId.toString() )
                
                                                                                    .addHeader(
                                                                                            S3HeaderType.CONTENT_LENGTH,
                                                                                            "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testFailsWithInvalidBlob()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                                       .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                                        .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), UUID.randomUUID()
                                                                  .toString() )
                                                                                    .addParameter(
                                                                                            RequestParameterType.JOB.toString(),
                                                                                            jobId.toString() )
                
                                                                                    .addHeader(
                                                                                            S3HeaderType.CONTENT_LENGTH,
                                                                                            "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }


    @Test
    public void testFailsWithInvalidJob()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                                       .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), user.getId(), JobRequestType.PUT )
                                        .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, bucket.getName() + "/" + object.getName() ).addParameter(
                        RequestParameterType.BLOB.toString(), blob.getObjectId()
                                                                  .toString() )
                                                                                    .addParameter(
                                                                                            RequestParameterType.JOB.toString(),
                                                                                            UUID.randomUUID()
                                                                                                .toString() )
                
                                                                                    .addHeader(
                                                                                            S3HeaderType.CONTENT_LENGTH,
                                                                                            "0" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }


    @Test
    public void testFailsWithNonLocalAuthentication()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), MockDaoDriver.DEFAULT_OBJECT_NAME, -1 );
        final Blob blob = mockDaoDriver.createBlobs( object.getId(), 1, 0 )
                                       .get( 0 );
        final UUID jobId = mockDaoDriver.createJob( null, null, JobRequestType.PUT )
                                        .getId();
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( support, true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ), RequestType.POST,
                bucket.getName() + "/" + object.getName() ).addParameter( RequestParameterType.BLOB.toString(),
                blob.getObjectId()
                    .toString() )
                                                           .addParameter( RequestParameterType.JOB.toString(),
                                                                   jobId.toString() )

                                                           .addHeader( S3HeaderType.CONTENT_LENGTH, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
