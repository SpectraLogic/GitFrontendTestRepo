/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.marshal.DateMarshaler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ModifyJobRequestHandler_Test 
{
    @Test
    public void testModifyJobCreatedAtIntoThePastAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        Date date = new Date( 987654 );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() ).addParameter(
                        JobObservable.CREATED_AT, String.valueOf( date.getTime() ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( DateMarshaler.marshal( date ) );

        date = new Date( 997654 );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() ).addParameter(
                        JobObservable.CREATED_AT, DateMarshaler.marshal( date ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( DateMarshaler.marshal( date ) );
    }
    
    
    @Test
    public void testModifyJobCreatedAtIntoTheFutureNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        final Date date = new Date( System.currentTimeMillis() + 1000 );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() ).addParameter(
                        JobObservable.CREATED_AT, String.valueOf( date.getTime() ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testModifyJobToNonUserSpecifiablePriorityNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() ).addParameter( 
                        JobObservable.PRIORITY, BlobStoreTaskPriority.CRITICAL.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testModifyJobReturnsJobInformation()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry(job.getId(),  blob );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );
        final String thisNodeIdString = serviceManager
                .getService( NodeService.class )
                .getThisNode()
                .getId()
                .toString();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        xPathIs( driver, "/MasterObjectList/@BucketName", MockDaoDriver.DEFAULT_BUCKET_NAME );
        xPathIs( driver, "/MasterObjectList/@CachedSizeInBytes", "0" );
        xPathIs( driver, "/MasterObjectList/@CompletedSizeInBytes", "0" );
        xPathIs( driver, "/MasterObjectList/@JobId", job.getId().toString() );
        xPathIs( driver, "/MasterObjectList/@OriginalSizeInBytes", "12345" );
        xPathIs( driver, "/MasterObjectList/@Priority", "URGENT" );
        xPathIs( driver, "/MasterObjectList/@RequestType", "PUT" );
        xPathIs( driver, "/MasterObjectList/@UserName", MockDaoDriver.DEFAULT_USER_NAME );

        xPathIs( driver, "count(/MasterObjectList/Nodes)", "1" );
        xPathIs( driver, "count(/MasterObjectList/Nodes/Node)", "1" );
        xPathIs( driver, "/MasterObjectList/Nodes/Node/@Id", thisNodeIdString );

        xPathIs( driver, "count(/MasterObjectList/Objects)", "1" );
        xPathIs( driver, "/MasterObjectList/Objects/@ChunkId", chunk.getId().toString() );
        xPathIs( driver, "/MasterObjectList/Objects/@ChunkNumber", "1" );
        xPathIs( driver, "count(/MasterObjectList/Objects/Object)", "1" );
        xPathIs( driver, "/MasterObjectList/Objects/Object/@InCache", "false" );
        xPathIs( driver, "/MasterObjectList/Objects/Object/@Offset", "0" );
        xPathIs( driver, "/MasterObjectList/Objects/Object/@Length", "12345" );
        xPathIs( driver, "/MasterObjectList/Objects/Object/@Name", MockDaoDriver.DEFAULT_OBJECT_NAME );

        final Job dbJob = mockDaoDriver.attain( Job.class, job.getId() );
        assertFalse(
                dbJob.isProtected(),
                "Job protected flag is not set" );
        assertTrue(
                dbJob.isDeadJobCleanupAllowed(),
                "Dead job cleanup allowed by default" );
    }


    @Test
    public void testEnableJobProtectedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/job/" + job.getId().toString() )
                .addParameter( Job.PROTECTED, "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Job dbJob = mockDaoDriver.attain( Job.class, job.getId() );
        assertTrue(
                dbJob.isProtected(),
                "Job protected flag is not set");
    }


    @Test
    public void testDisableJobProtectedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );

        mockDaoDriver.updateBean( job.setProtected( true ), Job.PROTECTED );

        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/job/" + job.getId().toString() )
                .addParameter( Job.PROTECTED, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Job dbJob = mockDaoDriver.attain( Job.class, job.getId() );
        assertFalse(
                dbJob.isProtected(),
                "Job protected flag is not set" );
    }


    @Test
    public void testDisableDeadJobCleanupAllowedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/job/" + job.getId().toString() )
                .addParameter( Job.DEAD_JOB_CLEANUP_ALLOWED, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Job dbJob = mockDaoDriver.attain( Job.class, job.getId() );
        assertFalse(
                dbJob.isDeadJobCleanupAllowed(),
                "Dead job cleanup not allowed");
    }


    @Test
    public void testEnableDeadJobCleanupAllowedFlagDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );

        mockDaoDriver.updateBean( job.setDeadJobCleanupAllowed( false ), Job.DEAD_JOB_CLEANUP_ALLOWED );

        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/job/" + job.getId().toString() )
                .addParameter( Job.DEAD_JOB_CLEANUP_ALLOWED, "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Job dbJob = mockDaoDriver.attain( Job.class, job.getId() );
        assertTrue(
                dbJob.isDeadJobCleanupAllowed(),
                "Dead job cleanup allowed" );
    }

    
    @Test
    public void testModifyJobWhenNoAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( mockDaoDriver.createUser( "su" ).getName() ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "su" ),
                RequestType.PUT, 
                "_rest_/job/" + job.getId().toString() ).addParameter( NameObservable.NAME, "blah" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    

    private static void xPathIs(
            final MockHttpRequestDriver driver,
            final String xPathExpression,
            final String expectedValue )
    {
        driver.assertResponseToClientXPathEquals( xPathExpression, expectedValue );
    }
}
