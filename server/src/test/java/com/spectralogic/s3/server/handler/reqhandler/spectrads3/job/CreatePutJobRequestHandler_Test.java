/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import org.apache.commons.lang3.StringUtils;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class CreatePutJobRequestHandler_Test 
{
    @Test
    public void testCreateEmptyJobNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects></Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreatePutJobCreatesPutJobWhenNoOptionalParametersProvided()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(2,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "Aggregating=\"false\"" );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        driver.assertResponseToClientDoesNotContain( "MinimizeSpanningAcrossMedia" );
        driver.assertResponseToClientContains( "NORMAL" );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(job.isReplicating(), "Shoulda configured job as a non-replicating one.");
        assertFalse(job.isNaked(), "Should notta been a naked job since job was created explicitly.");
        assertFalse(job.isImplicitJobIdResolution(), "Shoulda defaulted to no implicit job id resolution.");
        assertFalse(job.isMinimizeSpanningAcrossMedia(), "Shoulda defaulted to not minimizing spanning across media.");

        final CreatePutJobParams params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertFalse(params.isForce(), "Should notta sent create put job request down forcibly.");
        assertFalse(params.isPreAllocateJobSpace(), "Should not of sent create put job with pre-allocate.");
        assertFalse(job.isProtected(), "Should not have created job with protected flag");
        assertTrue(job.isDeadJobCleanupAllowed(), "Should have created job with dead job cleanup enabled");
    }
    

    @Test
    public void testCreatePutJobCreatesPutJobWhenAllOptionalParametersProvided()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName )
            .addParameter( CreatePutJobParams.PRE_ALLOCATE_JOB_SPACE, "" )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.LOW.toString() )
            .addParameter( Job.AGGREGATING, "true" )
            .addParameter( Job.MINIMIZE_SPANNING_ACROSS_MEDIA, "true" )
            .addParameter( Job.IMPLICIT_JOB_ID_RESOLUTION, "true" )
            .addParameter( "operation", "start_bulk_put" )
            .addParameter("protected", "true")
            .addParameter(Job.DEAD_JOB_CLEANUP_ALLOWED, "false");
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "Aggregating=\"true\"" );
        driver.assertResponseToClientDoesNotContain( "MinimizeSpanningAcrossMedia" );
        driver.assertResponseToClientContains( "LOW" );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertEquals(2,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        assertTrue(job.isImplicitJobIdResolution(), "Shoulda respected requested implicit job id resolution enabled.");
        assertTrue(job.isMinimizeSpanningAcrossMedia(), "Shoulda respected minimizing spanning across media specified.");
        final CreatePutJobParams params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertFalse(params.isForce(), "Should notta sent create put job request down forcibly.");
        assertTrue(params.isPreAllocateJobSpace(), "Should of sent create put job with pre-allocate.");
        assertTrue(job.isProtected(), "Should have created job with protected flag set to true");
        assertFalse(job.isDeadJobCleanupAllowed(), "Should have created job with dead job cleanup allowed disabled");
    }
    
    
    @Test
    public void testForceFlagOnlySentDownToCreatePutJobWhenTheJobCreationIsForced()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"2048\" />"
                + "<Object NAME=\"o2\" Size=\"2048\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        CreatePutJobParams params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertFalse(params.isForce(), "Should notta sent create put job request down forcibly.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( RequestParameterType.FORCE.toString(), "start_bulk_put" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 1 ).getArgs().get( 0 );
        assertTrue(params.isForce(), "Shoulda sent create put job request down forcibly.");

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.updateBean( 
                dataPolicy.setAlwaysForcePutJobCreation( true ),
                DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 2 ).getArgs().get( 0 );
        assertTrue(params.isForce(), "Shoulda sent create put job request down forcibly.");
    }
    

    @Test
    public void testCreateJobWithOfflineTargetSucceedsOnlyWhenTheJobCreationIsForced()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        mockDaoDriver.createDs3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target.getId() );
        mockDaoDriver.updateBean( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"2048\" />"
                + "<Object NAME=\"o2\" Size=\"2048\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta created non-forced job when ds3 target offline", 400 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );        
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( RequestParameterType.FORCE.toString(), "start_bulk_put" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should created forced job when ds3 target offline", 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.updateBean( 
                dataPolicy.setAlwaysForcePutJobCreation( true ),
                DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should created job using always-force data policy when ds3 target offline", 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
    }
    
    
    @Test
    public void testVerifyAfterWriteSentDownReflectsRequestIfSpecifiedElseDataPolicyDefault()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"2048\" />"
                + "<Object NAME=\"o2\" Size=\"2048\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        CreatePutJobParams params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertFalse(params.isVerifyAfterWrite(), "Should notta requested verify after write by default.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( Job.VERIFY_AFTER_WRITE, "true" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 1 ).getArgs().get( 0 );
        assertTrue(params.isVerifyAfterWrite(), "Shoulda sent down verify after write value correctly.");

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.updateBean( 
                dataPolicy.setDefaultVerifyAfterWrite( true ),
                DataPolicy.DEFAULT_VERIFY_AFTER_WRITE );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 2 ).getArgs().get( 0 );
        assertTrue(params.isVerifyAfterWrite(), "Shoulda sent down verify after write value correctly.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( Job.VERIFY_AFTER_WRITE, "false" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 3 ).getArgs().get( 0 );
        assertFalse(params.isVerifyAfterWrite(), "Shoulda sent down verify after write value correctly.");
    }

    
    @Test
    public void testMinimizeSpanningAcrossMediaFlagOnlySentDownToCreatePutJobWhenAppropriate()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"2048\" />"
                + "<Object NAME=\"o2\" Size=\"2048\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        CreatePutJobParams params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertFalse(params.isMinimizeSpanningAcrossMedia(), "Should notta sent create put job request down with minimize spanning.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( Job.MINIMIZE_SPANNING_ACROSS_MEDIA, "true" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 1 ).getArgs().get( 0 );
        assertTrue(params.isMinimizeSpanningAcrossMedia(), "Shoulda sent create put job request down with minimize spanning.");

        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.updateBean( 
                dataPolicy.setAlwaysMinimizeSpanningAcrossMedia( true ),
                DataPolicy.ALWAYS_MINIMIZE_SPANNING_ACROSS_MEDIA );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        mockDaoDriver.deleteAll( Job.class );
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        params = 
                (CreatePutJobParams)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 2 ).getArgs().get( 0 );
        assertTrue(params.isMinimizeSpanningAcrossMedia(), "Shoulda sent create put job request down with minimize spanning.");
    }
    
    
    @Test
    public void testCreatePutJobWithNameGeneratesJobWithName()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"2048\" />"
                + "<Object NAME=\"o2\" Size=\"2048\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( NameObservable.NAME, "proliant" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "proliant" );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertEquals("proliant", job.getName(), "Should notta had null name.");
    }
    
    
    @Test
    public void testCreatePutJobWithoutNameGeneratesJobWithDefaultName()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"2048\" />"
                + "<Object NAME=\"o2\" Size=\"2048\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "PUT by " );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertNotNull(
                "Should notta had null name.",
                job.getName() );
    }
    
    
    @Test
    public void testCreatePutJobWhenNotAllowingNewJobRequestsFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.updateBean( support.getDatabaseSupport()
                                         .getServiceManager()
                                         .getRetriever( DataPathBackend.class )
                                         .attain( Require.nothing() )
                                         .setAllowNewJobRequests( false ), DataPathBackend.ALLOW_NEW_JOB_REQUESTS );
        mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload =
                ( "<Objects>" + "<Object Name=\"o1\" Size=\"2048\" />" + "<Object NAME=\"o2\" Size=\"2048\" />" +
                        "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( support, true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ), RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreatePutJobWhenSafeDueToNonCriticalSystemFailureWithoutForceFlagAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( SystemFailure.class )
                .setType( SystemFailureType.CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION ) );
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(2,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
    }
    
    
    @Test
    public void testCreatePutJobWhenNotSafeWithoutForceFlagNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        mockDaoDriver.updateBean(
                target.setState( TargetState.OFFLINE ),
                ReplicationTarget.STATE );
        mockDaoDriver.createDs3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target.getId() );
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda notta added objects to the database.");
    }
    
    
    @Test
    public void testCreatePutJobWhenNotSafeWithForceFlagNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        mockDaoDriver.updateBean(
                target.setState( TargetState.OFFLINE ),
                ReplicationTarget.STATE );
        mockDaoDriver.createDs3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.values()[ 0 ], target.getId() );
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.addParameter( RequestParameterType.FORCE.toString(), "" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(2,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "Aggregating=\"false\"" );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        driver.assertResponseToClientDoesNotContain( "MinimizeSpanningAcrossMedia" );
        driver.assertResponseToClientContains( "NORMAL" );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(job.isNaked(), "Should notta been a naked job since job was created explicitly.");
        assertFalse(job.isMinimizeSpanningAcrossMedia(), "Shoulda defaulted to not minimizing spanning across media.");
    }
    
    
    @Test
    public void testInternalRequestWithoutUserImpersonationNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1/\" Size=\"0\" />"
                + "<Object Name=\"" + goodObjectName + "2/\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreatePutJobOfFoldersOnlyResultingInJobWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1/\" Size=\"0\" />"
                + "<Object Name=\"" + goodObjectName + "2/\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(2,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added folders to the database.");
    }
    
    
    @Test
    public void testCreatePutJobOfFoldersWithNonZeroSizeNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        mockDaoDriver.createBucket( userId, bucketName );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1/\" Size=\"1\" />"
                + "<Object Name=\"" + goodObjectName + "2/\" Size=\"0\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreatePutJobWhenOptionalParameterNotSpecifiedCorrectlyNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName ).addParameter( JobObservable.PRIORITY, "oops" );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Should notta added objects to the database due to failure.");
    }
    
    
    @Test
    public void testCreatePutJobWhenRequiredParameterIsMissingNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Should notta added objects to the database due to failure.");
    }
    
    
    @Test
    public void testCreatePutJobWithExtraUnknownAttributesOnObjectsDomElementNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects SomethingExtra=\"1\">"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" Size=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda not added any objects to the database since XML payload was invalid.");
    }
    
    
    @Test
    public void testCreatePutJobWithExtraUnknownAttributesOnObjectDomElementNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" SomethingExtra=\"1\" />"
                + "<Object Name=\"" + goodObjectName + "2\" Size=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda not added any objects to the database since XML payload was invalid.");
    }
    
    
    @Test
    public void testCreatePutJobWithExtraUnknownDomElementNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" Size=\"1024\" />"
                + "<Node Name=\"" + goodObjectName + "2\" Size=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda not added any objects to the database since XML payload was invalid.");
    }
    
    
    @Test
    public void testCreatePutJobCausesDataPolicyToBeIncompatibleWithFullLtfsCompWhenAnObjectNameIsTooLong()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        final String badObjectName = StringUtils.repeat( "123456789/", 102 ) + "123456";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final DataPolicyService dataPolicyService =
                support.getDatabaseSupport().getServiceManager().getService( DataPolicyService.class );
        assertTrue(dataPolicyService.areStorageDomainsWithObjectNamingAllowed(
                        mockDaoDriver.attainOneAndOnly( DataPolicy.class ) ), "Shoulda initialized data policy as allowing FULL ltfs compatibility.");
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "\" Size=\"2048\" />"
                + "<Object Name=\"" + badObjectName + "\" Size=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertFalse(dataPolicyService.areStorageDomainsWithObjectNamingAllowed(
                        mockDaoDriver.attainOneAndOnly( DataPolicy.class ) ), "Shoulda marked data policy as not allowing FULL ltfs compatibility.");
    }
    
    
    @Test
    public void testCreatePutJobWhenUserLacksPermissionToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.createObject( null, "o3", 0 );
        mockDaoDriver.createObject( null, "o4" );
        
        final User user =
                support.getDatabaseSupport().getServiceManager().getRetriever( User.class ).attain(
                        Require.nothing() );
        final Bucket bucket2 = mockDaoDriver.createBucket( user.getId(), "somebucket" );
        mockDaoDriver.addBucketAcl( bucket2.getId(), null, user.getId(), BucketAclPermission.OWNER );

        final String goodObjectName = "object";
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }


    @Test
    public void testCreatePutJobCreatesWithJsonRequestPayloadWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = ("{\"objects\":[" +
                "{\"name\":\"o1\",\"size\":2048}," +
                "{\"name\":\"o2\",\"size\":1024}" +
                "]}").getBytes(StandardCharsets.UTF_8);
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT,
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.addHeader(S3HeaderType.CONTENT_TYPE.getHttpHeaderName(), "application/json");
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(2,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        driver.assertResponseToClientContains( "\"ChunkClientProcessingOrderGuarantee\":\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "\"Aggregating\":false" );
        driver.assertResponseToClientContains( "\"EntirelyInCache\":false" );
        driver.assertResponseToClientDoesNotContain( "MinimizeSpanningAcrossMedia" );
        driver.assertResponseToClientContains( "NORMAL" );

        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(job.isReplicating(), "Shoulda configured job as a non-replicating one.");
        assertFalse(job.isNaked(), "Should notta been a naked job since job was created explicitly.");
        assertFalse(job.isImplicitJobIdResolution(), "Shoulda defaulted to no implicit job id resolution.");
        assertFalse(job.isMinimizeSpanningAcrossMedia(), "Shoulda defaulted to not minimizing spanning across media.");

        final CreatePutJobParams params =
                (CreatePutJobParams)
                        support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 );
        assertFalse(params.isForce(), "Should notta sent create put job request down forcibly.");
        assertFalse(params.isPreAllocateJobSpace(), "Should not of sent create put job with pre-allocate.");
    }


    @Test
    public void testCreatePutJobCreatesWithJsonRequestPayloadFolderWithSizeFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = ("{\"objects\":[" +
                "{\"name\":\"path/o1\",\"size\":2048}," +
                "{\"name\":\"path/folder/\",\"size\":1024}" +
                "]}").getBytes(StandardCharsets.UTF_8);
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT,
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.addHeader(S3HeaderType.CONTENT_TYPE.getHttpHeaderName(), "application/json");
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
}
