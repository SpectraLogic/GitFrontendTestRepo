/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.nio.charset.Charset;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class ReplicatePutJobRequestHandler_Test 
{
    @Test
    public void testReplicatePutJobWhenNoJobNeedsToBeCreatedReturns204()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.getDatabaseSupport().getServiceManager().getService( BucketService.class )
            .initializeLogicalSizeCache();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object o1 = mockDaoDriver.createObject( bucketId, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobToReplicate jobToReplicate = new JobReplicationSupport( 
                support.getDatabaseSupport().getServiceManager(), 
                chunk.getJobId() ).getJobToReplicate().getJob();
        
        final byte[] requestPayload = jobToReplicate.toJson(
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).getBytes( 
                        Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
    
    
    @Test
    public void testReplicatePutJobWhenNoOptionalParametersProvidedWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.getDatabaseSupport().getServiceManager().getService( BucketService.class )
            .initializeLogicalSizeCache();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object o1 = mockDaoDriver.createObject( bucketId, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobToReplicate jobToReplicate = new JobReplicationSupport( 
                support.getDatabaseSupport().getServiceManager(), 
                chunk.getJobId() ).getJobToReplicate().getJob();
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Job.class );
        
        final byte[] requestPayload = jobToReplicate.toJson(
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).getBytes( 
                        Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(1,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "NORMAL" );
    }
    
    
    @Test
    public void testReplicatePutJobWhenNotAllowingNewJobRequestsFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.updateBean( support.getDatabaseSupport()
                                         .getServiceManager()
                                         .getRetriever( DataPathBackend.class )
                                         .attain( Require.nothing() )
                                         .setAllowNewJobRequests( false ), DataPathBackend.ALLOW_NEW_JOB_REQUESTS );
    
        support.getDatabaseSupport().getServiceManager().getService( BucketService.class )
            .initializeLogicalSizeCache();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object o1 = mockDaoDriver.createObject( bucketId, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobToReplicate jobToReplicate = new JobReplicationSupport(
                support.getDatabaseSupport().getServiceManager(),
                chunk.getJobId() ).getJobToReplicate().getJob();
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Job.class );
        
        final byte[] requestPayload = jobToReplicate.toJson(
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).getBytes(
                        Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT,
                "_rest_/bucket/" + bucketName )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testReplicatePutJobCreatesJobWithDeadJobCleanupNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.getDatabaseSupport().getServiceManager().getService( BucketService.class )
            .initializeLogicalSizeCache();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object o1 = mockDaoDriver.createObject( bucketId, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobToReplicate jobToReplicate = new JobReplicationSupport( 
                support.getDatabaseSupport().getServiceManager(), 
                chunk.getJobId() ).getJobToReplicate().getJob();
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Job.class );
        
        final byte[] requestPayload = jobToReplicate.toJson(
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).getBytes( 
                        Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(1,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        assertFalse(
                support.getDatabaseSupport().getServiceManager().getRetriever( Job.class )
                        .attain( Require.nothing() ).isDeadJobCleanupAllowed(),
                "Should notta allowed dead job cleanup on replicated job."
                 );
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "NORMAL" );
    }
    
    
    @Test
    public void testReplicatePutJobWhenAllOptionalParametersProvidedWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.getDatabaseSupport().getServiceManager().getService( BucketService.class )
            .initializeLogicalSizeCache();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        final UUID bucketId = mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final S3Object o1 = mockDaoDriver.createObject( bucketId, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final JobEntry chunk = mockDaoDriver.createJobWithEntry( JobRequestType.PUT, b1 );
        final JobToReplicate jobToReplicate = new JobReplicationSupport( 
                support.getDatabaseSupport().getServiceManager(), 
                chunk.getJobId() ).getJobToReplicate().getJob();
        mockDaoDriver.deleteAll( Blob.class );
        mockDaoDriver.deleteAll( S3Object.class );
        mockDaoDriver.deleteAll( Job.class );
        
        final byte[] requestPayload = jobToReplicate.toJson(
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).getBytes( 
                        Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName )
            .addParameter( RequestParameterType.REPLICATE.toString(), "" )
            .addParameter( "operation", "start_bulk_put" )
            .addParameter( JobObservable.PRIORITY, "HIGH" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(1,  support.getDatabaseSupport().getDataManager().getCount(
                S3Object.class,
                Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId)), "Shoulda added objects to the database.");
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"IN_ORDER\"" );
        driver.assertResponseToClientContains( "HIGH" );
    }
}
