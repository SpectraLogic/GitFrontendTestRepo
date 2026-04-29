/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobsInCacheInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.EntriesInCacheInformation;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class GetJobChunksReadyForClientProcessingRequestHandler_Test 
{
    @BeforeEach
    protected void setUp() throws Exception
    {
        s_defaultPreferredNumberOfChunks =
                GetJobChunksReadyForClientProcessingRequestHandler.getDefaultPreferredNumberOfChunks();
        GetJobChunksReadyForClientProcessingRequestHandler.setDefaultPreferredNumberOfChunks( 2 );
    }


    @AfterEach
    protected void tearDown() throws Exception
    {
        GetJobChunksReadyForClientProcessingRequestHandler.setDefaultPreferredNumberOfChunks(
                s_defaultPreferredNumberOfChunks );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForPutJob()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testResultReturnedFilteredByChunkIdWhenReadyJobChunkRequestParamSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        support.setPlannerInterfaceIh(
                getPlannerInterfaceIh( new AtomicInteger( Integer.MAX_VALUE ), c1.getId() ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
            .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
            .addParameter( RequestParameterType.JOB_CHUNK.toString(), c3.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );

        final Method methodAllocate =
                ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" );
        final Method methodGetBlobsInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" );
        final Method methodIsChunkEntirelyInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "isChunkEntirelyInCache" );
        final Method methodCleanUp =
                ReflectUtil.getMethod( DataPlannerResource.class, "cleanUpCompletedJobsAndJobChunks" );
        final Method methodStillActive =
                ReflectUtil.getMethod( DataPlannerResource.class, "jobStillActive" );
        final Map< Method, Integer > expectedMethodCalls = new HashMap<>();
        expectedMethodCalls.put( methodAllocate, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodGetBlobsInCache, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodCleanUp, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodStillActive, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedMethodCalls );
    }


    @Test
    public void testResultReturnedFilteredByChunkIdWhenNotReadyJobChunkRequestParamSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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
        final JobEntry c5 = mockDaoDriver.createJobEntry( job2.getId(),
                b4 );

        mockDaoDriver.createBlobCache(c1.getBlobId(), CacheEntryState.IN_CACHE);

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
            .addParameter( RequestParameterType.JOB.toString(), job.getId().toString() )
            .addParameter( RequestParameterType.JOB_CHUNK.toString(), c1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 410 );
    }


    @Test
    public void testChunksFullyLoadedIntoCacheNotReturnedAsChunkAvailableForProcessingToClient()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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
        final JobEntry c5 = mockDaoDriver.createJobEntry( job2.getId(),
                b4 );

        mockDaoDriver.createBlobCache(c1.getBlobId(), CacheEntryState.IN_CACHE);

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
                .addParameter( "job", job.getId().toString() )
                .addParameter( RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(),"6" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2){
            driver.assertResponseToClientContains(e.getId().toString());
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );

        final Method methodAllocate =
                ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" );
        final Method methodGetBlobsInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" );
        final Method methodIsChunkEntirelyInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "isChunkEntirelyInCache" );
        final Method methodCleanUp =
                ReflectUtil.getMethod( DataPlannerResource.class, "cleanUpCompletedJobsAndJobChunks" );
        final Method methodStillActive =
                ReflectUtil.getMethod( DataPlannerResource.class, "jobStillActive" );
        final Map< Method, Integer > expectedMethodCalls = new HashMap<>();
        expectedMethodCalls.put( methodAllocate, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodGetBlobsInCache, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodCleanUp, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodStillActive, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedMethodCalls );
    }


    @Test
    public void testRequestHandlerAllocatesChunksCorrectlyWhenNoPreferredNumberOfChunksSpecifiedAndEmulationEnabled()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPathBackend backend = mockDaoDriver.attainOneAndOnly(DataPathBackend.class);
        mockDaoDriver.updateBean(backend.setEmulateChunks(true), DataPathBackend.EMULATE_CHUNKS);
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final JobEntry c2a = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 1 ) );
        final JobEntry c2b = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 2 ) );
        final JobEntry c2c = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 3 ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getChunkId().toString() );
        driver.assertResponseToClientContains( c2a.getChunkId().toString() );
        driver.assertResponseToClientDoesNotContain( c2b.getChunkId().toString() );
        driver.assertResponseToClientDoesNotContain( c2c.getChunkId().toString() );
        driver.assertResponseToClientDoesNotContain( c3.getChunkId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getChunkId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getChunkId().toString() );

        final Method methodAllocate =
                ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" );
        final Method methodGetBlobsInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" );
        final Method methodIsChunkEntirelyInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "isChunkEntirelyInCache" );
        final Method methodCleanUp =
                ReflectUtil.getMethod( DataPlannerResource.class, "cleanUpCompletedJobsAndJobChunks" );
        final Method methodStillActive =
                ReflectUtil.getMethod( DataPlannerResource.class, "jobStillActive" );
        final Map< Method, Integer > expectedMethodCalls = new HashMap<>();
        expectedMethodCalls.put( methodAllocate, Integer.valueOf( 2 ) );
        expectedMethodCalls.put( methodGetBlobsInCache, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodCleanUp, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodStillActive, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedMethodCalls );
    }


    @Test
    public void testRequestHandlerReturnsChunksForGetJobWhenEmulationEnabled()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPathBackend backend = mockDaoDriver.attainOneAndOnly(DataPathBackend.class);
        mockDaoDriver.updateBean(backend.setEmulateChunks(true), DataPathBackend.EMULATE_CHUNKS);
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.NONE ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        // GET path filters to entries in COMPLETED state.
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                    e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ) );
        }
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        // GETs are always one-entry-per-chunk regardless of the emulateChunks flag,
        // so the response should look the same as the emulation-off case: each
        // completed entry surfaces as its own chunk with chunkId == entryId.
        driver.assertResponseToClientContains( c1.getId().toString() );
        driver.assertResponseToClientContains( c3.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerAllocatesChunksCorrectlyWhenChunksEntirelyInCacheButNotAllocatedBlkp2810()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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
        final JobEntry c5 = mockDaoDriver.createJobEntry( job2.getId(),
                b4 );
        support.setPlannerInterfaceIh( getPlannerInterfaceIh( new AtomicInteger( 10 ), c1.getId() ) );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() ).addParameter(
                        RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(),
                        "6" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (JobEntry e : c2) {
            driver.assertResponseToClientContains( e.getId().toString() );
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );

        final Method methodAllocate =
                ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" );
        final Method methodGetBlobsInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" );
        final Method methodIsChunkEntirelyInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "isChunkEntirelyInCache" );
        final Method methodCleanUp =
                ReflectUtil.getMethod( DataPlannerResource.class, "cleanUpCompletedJobsAndJobChunks" );
        final Method methodStillActive =
                ReflectUtil.getMethod( DataPlannerResource.class, "jobStillActive" );
        final Map< Method, Integer > expectedMethodCalls = new HashMap<>();
        expectedMethodCalls.put( methodAllocate, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodGetBlobsInCache, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodCleanUp, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodStillActive, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedMethodCalls );
    }


    @Test
    public void testRequestHandlerAllocatesChunksCorrectlyWhenPreferredNumberOfChunksSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy(MockDaoDriver.DEFAULT_USER_NAME),
                RequestType.GET,
                "_rest_/job_chunk").addParameter("job", job.getId().toString()).addParameter(
                RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(),
                "6").addParameter(
                RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(),
                "6");
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (JobEntry e : c2) {
            driver.assertResponseToClientContains( e.getId().toString() );
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );

        final Method methodAllocate =
                ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" );
        final Method methodGetBlobsInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" );
        final Method methodIsChunkEntirelyInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "isChunkEntirelyInCache" );
        final Method methodCleanUp =
                ReflectUtil.getMethod( DataPlannerResource.class, "cleanUpCompletedJobsAndJobChunks" );
        final Method methodStillActive =
                ReflectUtil.getMethod( DataPlannerResource.class, "jobStillActive" );
        final Map< Method, Integer > expectedMethodCalls = new HashMap<>();
        expectedMethodCalls.put( methodAllocate, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodGetBlobsInCache, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodCleanUp, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodStillActive, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedMethodCalls );
    }


    @Test
    public void
    testRequestHandlerAllocatesChunksCorrectlyWhenPreferredNumberOfChunksSpecifiedAndCannotAllocateAllChunks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh( new AtomicInteger( 1 ) ) );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() ).addParameter(
                        RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(),
                        "4" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );

        final Method methodAllocate =
                ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" );
        final Method methodGetBlobsInCache =
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" );
        final Method methodCleanUp =
                ReflectUtil.getMethod( DataPlannerResource.class, "cleanUpCompletedJobsAndJobChunks" );
        final Method methodStillActive =
                ReflectUtil.getMethod( DataPlannerResource.class, "jobStillActive" );
        final Map< Method, Integer > expectedMethodCalls = new HashMap<>();
        expectedMethodCalls.put( methodAllocate, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodGetBlobsInCache, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodCleanUp, Integer.valueOf( 1 ) );
        expectedMethodCalls.put( methodStillActive, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedMethodCalls );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForGetJobWhenNoChunksReady()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( JobEntry.class ).setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ),
                JobEntry.BLOB_STORE_STATE );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForNonOrderGuaranteedGetJobWhenResultsNotTruncated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.NONE ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForOrderGuaranteedGetJobWhenResultsNotTruncated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.IN_ORDER ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientDoesNotContain( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRechunkedNonOrderGuaranteedGetJobAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.NONE ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setRechunked( new Date() ),
                JobObservable.RECHUNKED );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForGetJobWhenNotAllChunksContainJobEntries()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntry( job2.getId() );
        mockDaoDriver.createJobEntry( job3.getId() );

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
        final JobEntry c5 = mockDaoDriver.createJobEntry( job2.getId(),
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForNonOrderGuaranteedGetJobWhenResultsTruncated()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.NONE ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final JobEntry c2a = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 1 ) );
        final JobEntry c2b = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 2 ) );
        final JobEntry c2c = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 3 ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c2a.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c2b.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c2c.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        driver.assertResponseToClientContains( c2a.getId().toString() );
        driver.assertResponseToClientContains( c2b.getId().toString() );
        driver.assertResponseToClientContains( c2c.getId().toString() );
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForOrderGuaranteedGetJobWhenResultsTruncated1()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.IN_ORDER ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c4.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
            .addParameter( "job", job.getId().toString() ).addParameter(
                    RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(), "4" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientDoesNotContain( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForOrderGuaranteedGetJobWhenResultsTruncated2()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.IN_ORDER ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet(JobEntry.BLOB_STORE_STATE),
                    e.setBlobStoreState(JobChunkBlobStoreState.COMPLETED));
        }
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c4.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
            .addParameter( "job", job.getId().toString() ).addParameter(
                    RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(), "6" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2){
            driver.assertResponseToClientContains(e.getId().toString());
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForOrderGuaranteedGetJobWhenResultsTruncated3()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.IN_ORDER ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );


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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet(JobEntry.BLOB_STORE_STATE),
                    e.setBlobStoreState(JobChunkBlobStoreState.COMPLETED) );
        }
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c4.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
            .addParameter( "job", job.getId().toString() ).addParameter(
                    RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientContains(e.getId().toString());
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturnsCorrectResponseForOrderGuaranteedGetJobWhenResultsTruncated4()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job3 = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( Job.class ).setChunkClientProcessingOrderGuarantee(
                        JobChunkClientProcessingOrderGuarantee.IN_ORDER ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );

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
        final JobEntry c5 = mockDaoDriver.createJobEntry( job2.getId(),
                b4 );

        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        mockDaoDriver.delete( JobEntry.class, c2 );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c5.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" )
            .addParameter( "job", job.getId().toString() ).addParameter(
                    RequestParameterType.PREFERRED_NUMBER_OF_CHUNKS.toString(), "4" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( job.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( job3.getId().toString() );

        driver.assertResponseToClientContains( c1.getId().toString() );
        for (final JobEntry e : c2) {
            driver.assertResponseToClientDoesNotContain(e.getId().toString());
        }
        driver.assertResponseToClientContains( c3.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c4.getId().toString() );
        driver.assertResponseToClientDoesNotContain( c5.getId().toString() );
    }


    @Test
    public void testRequestHandlerReturns200WithRetryAfterWhenNoChunksAvailable()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh( new AtomicInteger( 0 ) ) );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
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

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver.assertResponseToClientXPathEquals( "count(/MasterObjectList)", "1" );
        driver.assertResponseToClientXPathEquals( "count(/MasterObjectList/Objects)", "0" );

        final Map< String, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( "Retry-After", "60" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }


    @Test
    public void testRequestHandlerReturns410WhenNoChunksRemaining()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh( new AtomicInteger( 0 ) ) );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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

        final JobEntry c1 = mockDaoDriver.createJobEntry(job.getId(),
                blobs.get( 0 ) );
        final Set<JobEntry> c2 = mockDaoDriver.createJobEntries(job.getId(),
                CollectionFactory.toSet( blobs.get( 1 ), blobs.get( 2 ), blobs.get( 3 ) ) );
        final JobEntry c3 = mockDaoDriver.createJobEntry(job.getId(),
                b2 );
        final JobEntry c4 = mockDaoDriver.createJobEntry(job2.getId(),
                b3 );
        final JobEntry c5 = mockDaoDriver.createJobEntry(job3.getId(),
                b4 );

        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c1.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );
        for (final JobEntry e : c2) {
            support.getDatabaseSupport().getDataManager().updateBean(
                    CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                    e.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ) );
        }
        support.getDatabaseSupport().getDataManager().updateBean(
                CollectionFactory.toSet( JobEntry.BLOB_STORE_STATE ),
                c3.setBlobStoreState( JobChunkBlobStoreState.COMPLETED ) );

        assignNodeIds( mockDaoDriver );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 410 );

        final Map< String, String > requiredHeaders = new HashMap<>();
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }


    @Test
    public void testRequestHandlerReturns404WhenJobDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        new MockDaoDriver( support.getDatabaseSupport() ).createUser( "test_user" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( "test_user" ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", "9e5c09e8-239a-4d37-a918-26829959d58d" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }


    @Test
    public void testRequestHandlerReturns403WhenUserDoesNotHaveAccess()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        support.setPlannerInterfaceIh( getPlannerInterfaceIh() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createJob( null, null, JobRequestType.GET );
        mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
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
        final JobEntry c5 = mockDaoDriver.createJobEntry( job2.getId(),
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

        assignNodeIds( mockDaoDriver );
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( mockDaoDriver.createUser( "user2" ).getName() ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET,
                "_rest_/job_chunk" ).addParameter( "job", job.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }


    private InvocationHandler getPlannerInterfaceIh()
    {
        return getPlannerInterfaceIh( new AtomicInteger( Integer.MAX_VALUE ) );
    }


    private InvocationHandler getPlannerInterfaceIh(
            final AtomicInteger maxChunksToAllocate,
            final UUID ... chunksEntirelyInCache )
    {
        return MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobsInCache" ),
                new ConstantResponseInvocationHandler( new RpcResponse<>(
                        BeanFactory.newBean( BlobsInCacheInformation.class ) ) ),
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntry" ),
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args )
                                    throws Throwable
                            {
                                if ( 0 > maxChunksToAllocate.decrementAndGet() )
                                {
                                    throw new RpcProxyException(
                                            "oops", BeanFactory.newBean( Failure.class ) );
                                }
                                return new RpcResponse<>( null );
                            }
                        },
                        MockInvocationHandler.forMethod(
                                ReflectUtil.getMethod( DataPlannerResource.class, "isChunkEntirelyInCache" ),
                                new InvocationHandler()
                                {
                                    public Object invoke( Object proxy, Method method, Object[] args )
                                            throws Throwable
                                    {
                                        return new RpcResponse<>( Boolean.valueOf( CollectionFactory.toSet(
                                                chunksEntirelyInCache ).contains( args[ 0 ] ) ) );
                                    }
                                },
                                MockInvocationHandler.forMethod(
                                        ReflectUtil.getMethod( DataPlannerResource.class, "allocateEntries" ),
                                        new InvocationHandler()
                                        {
                                            public Object invoke( Object proxy, Method method, Object[] args )
                                                    throws Throwable
                                            {
                                                final List<UUID> returnEntries = new ArrayList();
                                                for (final UUID entry : (UUID[]) args[ 1 ] ) {
                                                    if ( 0 > maxChunksToAllocate.decrementAndGet() )
                                                    {
                                                        break;
                                                    }
                                                    returnEntries.add( entry );
                                                }
                                                //return list as typed array
                                                final UUID[] entryIds = returnEntries.toArray( new UUID[ returnEntries.size() ] );
                                                final EntriesInCacheInformation response = BeanFactory.newBean( EntriesInCacheInformation.class );
                                                response.setEntriesInCache( entryIds );
                                                return new RpcResponse<>( response );


                                            }
                                        }, null
                                ) ) ) );
    }


    private void assignNodeIds( final MockDaoDriver mockDaoDriver )
    {
        final Set<UUID> blobIdsAdded = new HashSet<>();
        for (final DetailedJobEntry entry : mockDaoDriver.retrieveAll( DetailedJobEntry.class )) {
            if (entry.getCacheState() == null && !blobIdsAdded.contains(entry.getBlobId())) {
                if (entry.getRequestType() == JobRequestType.PUT) {
                    mockDaoDriver.createBlobCache(entry.getBlobId(), CacheEntryState.ALLOCATED);
                } else {
                    mockDaoDriver.createBlobCache(entry.getBlobId(), CacheEntryState.ALLOCATED);
                }
                blobIdsAdded.add(entry.getBlobId());
            }
        }
    }

    private static RpcResponse<EntriesInCacheInformation> entryResponse( final UUID ... entryIds )
    {
        final EntriesInCacheInformation response = BeanFactory.newBean( EntriesInCacheInformation.class );
        response.setEntriesInCache( entryIds );
        return new RpcResponse<>( response );
    }

    private static int s_defaultPreferredNumberOfChunks;
}
