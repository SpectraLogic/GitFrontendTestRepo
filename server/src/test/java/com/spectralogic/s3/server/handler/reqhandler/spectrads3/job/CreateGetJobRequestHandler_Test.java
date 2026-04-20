/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreateGetJobParams;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class CreateGetJobRequestHandler_Test 
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
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWhereBlobsSpecifiedByIdWithInvalidIdNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createObject( null, "o3", 0 );
        mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + b1.getId() + "\" />"
                + "<Object NAME=\"" + b2.getId() + "\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addHeader( S3HeaderType.SPECIFY_BY_ID, "" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 500 );
    }
    
    
    @Test
    public void testCreateGetJobWhereBlobsSpecifiedByIdWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.createObject( null, "o3", 0 );
        mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final UUID jobId = UUID.randomUUID();
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + b1.getId() + "\" />"
                + "<Object NAME=\"" + b2.getId() + "\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addHeader( S3HeaderType.SPECIFY_BY_ID, jobId.toString() );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"NONE\"" );
        driver.assertResponseToClientContains( "Aggregating=\"false\"" );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        driver.assertResponseToClientContains( "HIGH" );
        driver.assertResponseToClientContains( jobId.toString() );
        
        driver.assertResponseToClientContains( "o1" );
        driver.assertResponseToClientContains( "o2" );
        driver.assertResponseToClientDoesNotContain( "o3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        driver.assertResponseToClientContains( b1.getId().toString() );
        driver.assertResponseToClientContains( b2.getId().toString() );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(job.isNaked(), "Should notta been a naked job since job was created explicitly.");
    }
    
    
    @Test
    public void testCreateGetJobCreatesGetJobWhenNoOptionalParametersSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.createObject( null, "o3", 0 );
        mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"NONE\"" );
        driver.assertResponseToClientContains( "Aggregating=\"false\"" );
        driver.assertResponseToClientContains( "EntirelyInCache=\"false\"" );
        driver.assertResponseToClientContains( "HIGH" );
        driver.assertResponseToClientContains( "o1" );
        driver.assertResponseToClientContains( b1.getId().toString() );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertFalse(job.isNaked(), "Should notta been a naked job since job was created explicitly.");
        assertFalse(job.isImplicitJobIdResolution(), "Should notta enabled implicit job id resolution by default.");
        assertFalse(job.isProtected(), "Should not have protected flag set.");
        assertTrue(
                job.isDeadJobCleanupAllowed(),
                "Should have dead job cleanup enabled by default"
                 );
    }
    
    
    @Test
    public void testCreateGetJobWithNameGeneratesJobWithName()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( NameObservable.NAME, "proliant" );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "proliant" );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertEquals("proliant", job.getName(), "Should notta had null name.");
    }
    
    
    @Test
    public void testCreateGetJobWithoutNameGeneratesJobWithDefaultName()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "GET by " );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertNotNull(job.getName(), "Should notta had null name.");
    }


    @Test
    public void testCreateGetJobWithJsonRequestPayloadWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = ("{\"objects\":[" +
                "{\"name\":\"o1\"}," +
                "{\"name\":\"o2\",\"length\":5,\"offset\":2}" +
                "]}").getBytes(StandardCharsets.UTF_8);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addHeader(S3HeaderType.CONTENT_TYPE.getHttpHeaderName(), "application/json");
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "GET by " );
        driver.assertResponseToClientContains( "o1" );
        driver.assertResponseToClientContains( "o2" );

        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertNotNull(job.getName(), "Should not have had null name.");
    }


    @Test
    public void testCreateGetJobWithJsonRequestPayloadNegativeLengthFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = ("{\"objects\":[" +
                "{\"name\":\"o1\",\"length\":5}," +
                "{\"name\":\"o2\",\"length\":-1}" +
                "]}").getBytes(StandardCharsets.UTF_8);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addHeader(S3HeaderType.CONTENT_TYPE.getHttpHeaderName(), "application/json");
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }


    @Test
    public void testCreateGetJobWithJsonRequestPayloadNegativeOffsetFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = ("{\"objects\":[" +
                "{\"name\":\"o1\",\"offset\":2}," +
                "{\"name\":\"o2\",\"offset\":-1}" +
                "]}").getBytes(StandardCharsets.UTF_8);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addHeader(S3HeaderType.CONTENT_TYPE.getHttpHeaderName(), "application/json");
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobIncludesZeroLengthObjects()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.createObject( null, "o3", 0 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "<Object NAME=\"o3\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "o1" );
        driver.assertResponseToClientContains( "o2" );
        driver.assertResponseToClientContains( "o3" );
        driver.assertResponseToClientDoesNotContain( "o4" );
        
        assertBlobsSentToDataPlannerAsExpected( 
            support.getDatabaseSupport().getServiceManager().getRetriever( Blob.class )
             .retrieveAll( Require.not( Require.beanPropertyEquals( Identifiable.ID, b4.getId() ) ) ).toSet(),
            support );
    }
    
    
    @Test
    public void testCreateGetJobThatIncludesObjectsThatDontExistNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        assertTrue(
                driver.getResponseToClientAsString().contains( "o2" ),
                "Exception message shoulda included o2, which was missing, but not o1, which was present."
                 );
        assertFalse(driver.getResponseToClientAsString().contains( "o1" ), "Exception message shoulda included o2, which was missing, but not o1, which was present.");
    }
    
    
    @Test
    public void testCreateGetJobWithInvalidLengthAndOffsetResultingInNoBlobsNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"22\" Offset=\"33\" />"
                + "<Object NAME=\"o2\" LENGTH=\"33\" offset=\"44\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWithNegativeOffsetNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"2\" Offset=\"-3\" />"
                + "<Object NAME=\"o2\" LENGTH=\"3\" offset=\"4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWithNegativeLengthNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"-2\" Offset=\"3\" />"
                + "<Object NAME=\"o2\" LENGTH=\"3\" offset=\"4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWithFoldersAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.createObject( null, "folder/" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"folder/\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testCreateGetJobWhenNotAllowingNewJobRequestsFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
//        mockDaoDriver.updateBean( support.getDatabaseSupport()
//                                         .getServiceManager()
//                                         .getRetriever( DataPathBackend.class )
//                                         .attain( Require.nothing() )
//                                         .setAllowNewJobRequests( false ), DataPathBackend.ALLOW_NEW_JOB_REQUESTS );
        
        mockDaoDriver.createObject( null, "no-new-jobs" );
        
        final byte[] requestPayload =
                ( "<Objects><Object Name='no-new-jobs'/></Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( support, true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ), RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testCreateGetJobCreatesGetJobWhenAllOptionalParametersSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"2\" Offset=\"3\" />"
                + "<Object NAME=\"o2\" LENGTH=\"3\" offset=\"4\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.LOW.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.NONE.toString() )
            .addParameter( Job.AGGREGATING, "true" )
            .addParameter( Job.IMPLICIT_JOB_ID_RESOLUTION, "true" );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"NONE\"" );
        driver.assertResponseToClientContains( "Aggregating=\"true\"" );
        driver.assertResponseToClientContains( "LOW" );
        
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        assertTrue(
                job.isImplicitJobIdResolution(),
                "Shoulda respected requested implicit job id resolution enabled."
                 );
    }
    
    
    @Test
    public void testPartialObjectGetNoLengthResultsInGettingAllBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( new HashSet<>( blobs1 ), support );
    }
    
    
    @Test
    public void testPartialObjectGetZeroLengthResultsInGettingAllBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"0\" Offset=\"20\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( new HashSet<>( blobs1 ), support );
    }
    
    
    @Test
    public void testPartialObjectGetWhenObjectNotYetCreatedAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"10\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testPartialObjectGetWithPositiveLengthOfOneBlobAndNoOffsetSendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"10\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( CollectionFactory.toSet( blobs1.get( 0 ) ), support );
    }
    
    
    @Test
    public void testPartialObjectGetWithPositiveLengthOfMultipleBlobsAndNoOffsetSendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"11\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( 
                CollectionFactory.toSet( blobs1.get( 0 ), blobs1.get( 1 ) ), support );
    }
    
    
    @Test
    public void testPartialObjectGetWithPositiveLengthOfOneBlobAndOffsetOffBlobBoundarySendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"1\" Offset=\"19\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( CollectionFactory.toSet( blobs1.get( 1 ) ), support );
    }
    
    
    public void
    testPartialObjectGetWithPositiveLengthOfMultipleBlobsAndOffsetOffBlobBoundarySendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"2\" Offset=\"19\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( 
                CollectionFactory.toSet( blobs1.get( 1 ), blobs1.get( 2 ) ), support );
    }
    
    
    @Test
    public void testPartialObjectGetWithPositiveLengthOfOneBlobAndOffsetOnBlobBoundarySendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"10\" Offset=\"10\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( CollectionFactory.toSet( blobs1.get( 1 ) ), support );
    }
    
    
    @Test
    public void 
    testPartialObjectGetWithPositiveLengthOfMultipleBlobsAndOffsetOnBlobBoundarySendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"11\" Offset=\"10\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertBlobsSentToDataPlannerAsExpected( 
                CollectionFactory.toSet( blobs1.get( 1 ), blobs1.get( 2 ) ), support );
    }
    
    
    @Test
    public void testPartialObjectGetWithMultiPartRequestsWhereSingleByteRangeInvalidNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"11\" Offset=\"10\" />"
                + "<Object Name=\"o2\" />"
                + "<Object Name=\"o3\" Length=\"10\" />"
                + "<Object Name=\"o3\" Length=\"71\" Offset=\"30\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testPartialObjectGetWithMultiPartRequestSendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o3\" Length=\"10\" />"
                + "<Object Name=\"o3\" Length=\"10\" Offset=\"30\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                    JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< Blob > expectedBlobs = new HashSet<>();
        expectedBlobs.add( blobs3.get( 0 ) );
        expectedBlobs.add( blobs3.get( 3 ) );
        assertBlobsSentToDataPlannerAsExpected( expectedBlobs, support );
    }
    
    
    @Test
    public void testPartialObjectGetWithMultiPartRequestsSendsCorrectBlobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final List< Blob > blobs1 = mockDaoDriver.createBlobs( o1.getId(), 10, 10 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o2.getId(), 10, 10 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 10, 10 );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o4.getId(), 10, 10 );
        assertEquals(40,  (blobs1.size() + blobs2.size() + blobs3.size() + blobs4.size()), "Shoulda created 10 blobs for each object.");
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Length=\"11\" Offset=\"10\" />"
                + "<Object Name=\"o2\" />"
                + "<Object Name=\"o3\" Length=\"10\" />"
                + "<Object Name=\"o3\" Length=\"10\" Offset=\"30\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME )
            .addParameter( JobObservable.PRIORITY, BlobStoreTaskPriority.HIGH.toString() )
            .addParameter( JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                           JobChunkClientProcessingOrderGuarantee.IN_ORDER.toString() );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< Blob > expectedBlobs = new HashSet<>();
        expectedBlobs.add( blobs1.get( 1 ) );
        expectedBlobs.add( blobs1.get( 2 ) );
        expectedBlobs.addAll( blobs2 );
        expectedBlobs.add( blobs3.get( 0 ) );
        expectedBlobs.add( blobs3.get( 3 ) );
        assertBlobsSentToDataPlannerAsExpected( expectedBlobs, support );
    }
    
    
    private void assertBlobsSentToDataPlannerAsExpected( 
            final Set< Blob > expectedBlobs,
            final MockHttpRequestSupport support )
    {
        final List< UUID > expectedIds = new ArrayList<>( BeanUtils.toMap( expectedBlobs ).keySet() );
        Collections.sort( expectedIds );
        
        final CreateGetJobParams params = 
                (CreateGetJobParams)support.getPlannerInterfaceBtih().getMethodInvokeData( 
                ReflectUtil.getMethod( DataPlannerResource.class, "createGetJob" ) )
                .get( 0 ).getArgs().get( 0 );
        final List< UUID > actualIds = 
                CollectionFactory.toList( params.getBlobIds() );
        Collections.sort( actualIds );

        assertEquals(expectedIds, actualIds, "Shoulda sent correct blob list.");
    }
    
    
    @Test
    public void testCreateGetJobWhereRequiredParameterIsMissingNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object/>"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWithExtraUnknownAttributesOnObjectsDomElementNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects InvalidParam=\"1024\">"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWithExtraUnknownAttributesOnObjectDomElementNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" Size=\"1024\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWithExtraUnknownDomElementNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "<Node Name=\"o1\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testCreateGetJobWhenUserLacksPermissionToDoSoNotAllowed()
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
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }


    @Test
    public void testCreateGetJobWithProtectedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = (
                "<Objects>"
                        + "<Object Name=\"o1\" />"
                        + "<Object NAME=\"o2\" />"
                        + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addParameter( Job.PROTECTED, "true" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }

    @Test
    public void testCreateGetJobWithDeadJobCleanupDisallowedDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = (
                "<Objects>"
                        + "<Object Name=\"o1\" />"
                        + "<Object NAME=\"o2\" />"
                        + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_get" );
        driver.addParameter( Job.DEAD_JOB_CLEANUP_ALLOWED, "false" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
