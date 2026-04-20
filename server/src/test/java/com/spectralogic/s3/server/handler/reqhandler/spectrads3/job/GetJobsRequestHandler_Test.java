/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkBlobStoreState;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.domain.JobStatus;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class GetJobsRequestHandler_Test 
{
    @Test
    public void testRequestHandlerReturnsCorrectResponseForJobs()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job completedJob = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final CanceledJob canceledJob = mockDaoDriver.createCanceledJob( null, null, null, null );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        TestUtil.sleep( 10 );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.createObject( null, "o5" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 4, 1000 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        
        final JobEntry c1 = mockDaoDriver.createJobEntry( job.getId(),
                blobs.get( 0 ));
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries( job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry( job.getId(),
                 b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry( job2.getId(),
                 b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry( job3.getId(),
                 b4 );
        
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.PENDING ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                    e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ) );
        }
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/job" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientContains( job2.getId().toString() );
        driver.assertResponseToClientContains( job3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( completedJob.getId().toString() );
        driver.assertResponseToClientDoesNotContain( canceledJob.getId().toString() );
        assertTrue(
                driver.getResponseToClientAsString().indexOf( job.getId().toString() )
                        > driver.getResponseToClientAsString().indexOf( job2.getId().toString() ),
                "Shoulda sorted job2 before job1 since job2 was created more recently."
                );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
        
        driver.assertResponseToClientContains( JobStatus.IN_PROGRESS.toString() );
        driver.assertResponseToClientDoesNotContain( JobStatus.COMPLETED.toString() );
        driver.assertResponseToClientDoesNotContain( JobStatus.CANCELED.toString() );
    }
    
    
    @Test
    public void testRequestHandlerReturnsCorrectResponseForJobsWhenFilteringByBucket()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "otherbucket" );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job completedJob = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final CanceledJob canceledJob = mockDaoDriver.createCanceledJob( null, null, null, null );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        TestUtil.sleep( 10 );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.createObject( null, "o5" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 4, 1000 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        
        final JobEntry c1 = mockDaoDriver.createJobEntry( job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries( job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry( job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry( job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry( job3.getId(),
                b4 );
        
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.PENDING ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                    e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ) );
        }
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/job" ).addParameter( JobObservable.BUCKET_ID, MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientContains( job2.getId().toString() );
        driver.assertResponseToClientContains( job3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( completedJob.getId().toString() );
        driver.assertResponseToClientDoesNotContain( canceledJob.getId().toString() );
        assertTrue(
                driver.getResponseToClientAsString().indexOf( job.getId().toString() )
                        > driver.getResponseToClientAsString().indexOf( job2.getId().toString() ),
                "Shoulda sorted job2 before job1 since job2 was created more recently."
               );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
        
        driver.assertResponseToClientContains( JobStatus.IN_PROGRESS.toString() );
        driver.assertResponseToClientDoesNotContain( JobStatus.COMPLETED.toString() );
        driver.assertResponseToClientDoesNotContain( JobStatus.CANCELED.toString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/job" ).addParameter( JobObservable.BUCKET_ID, bucket2.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( completedJob.getId().toString() );
        driver.assertResponseToClientDoesNotContain( canceledJob.getId().toString() );
    }
    
    
    @Test
    public void testRequestHandlerReturnsCorrectResponseForJobsWhenFullDetails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job completedJob = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final CanceledJob canceledJob = mockDaoDriver.createCanceledJob( null, null, null, null );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        TestUtil.sleep( 10 );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.createObject( null, "o5" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 4, 1000 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        
        final JobEntry c1 = mockDaoDriver.createJobEntry( job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries( job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry( job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry( job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry( job3.getId(),
                b4 );
        
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.PENDING ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                    e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ) );
        }
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/job" ).addParameter( RequestParameterType.FULL_DETAILS.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientContains( job2.getId().toString() );
        driver.assertResponseToClientContains( job3.getId().toString() );
        driver.assertResponseToClientContains( completedJob.getId().toString() );
        driver.assertResponseToClientContains( canceledJob.getId().toString() );
        assertTrue(
                driver.getResponseToClientAsString().indexOf( job.getId().toString() )
                        > driver.getResponseToClientAsString().indexOf( job2.getId().toString() ),
                "Shoulda sorted job2 before job1 since job2 was created more recently."
                );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
        
        driver.assertResponseToClientContains( JobStatus.IN_PROGRESS.toString() );
        driver.assertResponseToClientContains( JobStatus.COMPLETED.toString() );
        driver.assertResponseToClientContains( JobStatus.CANCELED.toString() );
    }
    
    
    @Test
    public void testRequestHandlerDoesNotReturnJobsUserDoesNotHaveAccessToSee()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user1" );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job completedJob = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final CanceledJob canceledJob = mockDaoDriver.createCanceledJob( null, user.getId(), null, null );
        final Job job = mockDaoDriver.createJob( null, user.getId(), JobRequestType.PUT );
        TestUtil.sleep( 10 );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", -1 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4" );
        mockDaoDriver.createObject( null, "o5" );
        
        final List< Blob > blobs = mockDaoDriver.createBlobs( o1.getId(), 4, 1000 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        
        final JobEntry c1 = mockDaoDriver.createJobEntry( job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries( job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry( job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry( job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry( job3.getId(),
                b4 );
        
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.PENDING ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                    e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ) );
        }
        support.getDatabaseSupport().getDataManager().updateBean( 
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/job" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( completedJob.getId().toString() );
        driver.assertResponseToClientDoesNotContain( canceledJob.getId().toString() );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
        
        driver.assertResponseToClientContains( JobStatus.IN_PROGRESS.toString() );
        driver.assertResponseToClientDoesNotContain( JobStatus.COMPLETED.toString() );
        driver.assertResponseToClientDoesNotContain( JobStatus.CANCELED.toString() );
    }
    
    
    private InvocationHandler getPlannerInterfaceIh()
    {
        return MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" ), 
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        throw new RuntimeException( "Getting all jobs should never call this." );
                    }
                },
                null );
    }
}
