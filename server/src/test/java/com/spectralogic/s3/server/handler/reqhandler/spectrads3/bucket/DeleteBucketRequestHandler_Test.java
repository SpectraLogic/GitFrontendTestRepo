/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobService;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.DeleteObjectsResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.mock.DeleteBucketThrowsBucketNotEmptyInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteBucketRequestHandler_Test 
{
    @Test
    public void testDeleteBucketReturns404WhenBucketDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/doesntexist" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testDeleteBucketReturns204WhenBucketExistsAndIsEmpty()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        new MockDaoDriver( support.getDatabaseSupport() )
                .createBucket( null, "test_bucket_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/test_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    
    
    @Test
    public void testDeleteBucketReturns409WhenBucketExistsAndIsNotEmpty()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setTargetInterfaceIh( MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DeleteObjectsResource.class, "deleteBucket" ),
                new DeleteBucketThrowsBucketNotEmptyInvocationHandler(),
                null ) );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/test_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }
    
    
    @Test
    public void testDeleteBucketDoesSoWhenBucketExistsAndIsNotEmptyAndForceFlagIsSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/test_bucket_name" ).addParameter( RequestParameterType.FORCE.toString(), "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    
    
    @Test
    public void testDeleteBucketThatUsesDataPolicyWithoutSecureBucketIsolatedPersistenceRulesAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(), 
                DataPersistenceRuleType.PERMANENT, 
                storageDomain.getId() );
        mockDaoDriver.createObject( bucketId, "test_object_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/test_bucket_name" ).addParameter( RequestParameterType.FORCE.toString(), "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    

    @Test
    public void testDeleteSpectraNamespaceBucketAllowedIfDoneAsInternalRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "blah" );
        final Bucket bucket = mockDaoDriver.createBucket(
                user.getId(), BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    

    @Test
    public void testDeleteSpectraNamespaceBucketAllowedIfDoneAsReplicatedRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "blah" );
        final Bucket bucket = mockDaoDriver.createBucket(
                user.getId(), BucketAuthorization.SYSTEM_BUCKET_NAME_PREFIX + "test" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() )
            .addHeader( S3HeaderType.REPLICATION_SOURCE_IDENTIFIER, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.BUCKET + "/" + bucket.getName() )
            .addHeader( S3HeaderType.REPLICATION_SOURCE_IDENTIFIER, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    
    
    @Test
    public void testDeleteBucketDelegatesToPlannerResourceWhenNotReplicated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        new MockDaoDriver( support.getDatabaseSupport() )
                .createBucket( null, "test_bucket_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/test_bucket_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                0,
                support.getPlannerInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target resource."
                 );
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target resource."
                );
    }
    
    
    @Test
    public void testDeleteBucketDelegatesToTargetResource()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        new MockDaoDriver( support.getDatabaseSupport() )
                .createBucket( null, "test_bucket_name" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/test_bucket_name" );
        
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                0,
                support.getPlannerInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target resource."
                 );
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target resource."
                 );
    }


    @Test
    public void testDeleteBucketReturns409WhenBucketProtected()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final Bucket bucket = new MockDaoDriver( support.getDatabaseSupport() )
                .createBucket( null, "test_bucket_name" );

        support.getDatabaseSupport().getServiceManager().getService( BucketService.class )
                .update( bucket.setProtected( true ), Bucket.PROTECTED );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }


    @Test
    public void testDeleteBucketAllowedWhenThereIsAnActiveJob()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final Bucket bucket = mockDaoDriver.createBucket( null, "test_bucket_name" );
        mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }


    @Test
    public void testDeleteBucketReturns409WhenItHasAProtectedActiveJob()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final Bucket bucket = mockDaoDriver.createBucket( null, "test_bucket_name" );
        final Job putJob = mockDaoDriver.createJob(bucket.getId(), null, JobRequestType.PUT);

        support.getDatabaseSupport().getServiceManager().getService( JobService.class )
                .update( putJob.setProtected( true ), Job.PROTECTED );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/bucket/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }
}
