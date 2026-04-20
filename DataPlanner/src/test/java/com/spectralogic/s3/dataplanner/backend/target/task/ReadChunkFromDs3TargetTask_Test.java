/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.testfrmwrk.TargetTaskBuilder;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public final class ReadChunkFromDs3TargetTask_Test 
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TargetTaskBuilder(null).buildReadChunkFromDs3TargetTask(
                        new ReadDirective(
                                BlobStoreTaskPriority.values()[0],
                                UUID.randomUUID(),
                                PersistenceType.DS3,
                                new ArrayList<>()
                        ));
            }
        } );
    }


    @Test
    public void testHappyConstruction()
    {
        new TargetTaskBuilder(InterfaceProxyFactory.getProxy(BeansServiceManager.class, null)).buildReadChunkFromDs3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.values()[0],
                        UUID.randomUUID(),
                        PersistenceType.DS3,
                        new ArrayList<>()
                ));
    }
    
    
    @Test
    public void testGetChunkReturnsChunk()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromDs3TargetId( target.getId() ),
                    ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        }
        final ReadChunkFromDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildReadChunkFromDs3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.values()[0],
                        target.getId(),
                        PersistenceType.DS3,
                        new ArrayList<>(chunks)
                ));
        final Object expected = chunks.iterator().next();
        assertEquals(expected, task.getEntries().iterator().next(), "Shoulda returned chunk.");
    }
    

    @Test
    public void testPrepareForExecutionWhenJobChunkNoLongerExistsNotPossible()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final JobEntry chunk =
                mockDaoDriver.createJobWithEntry( JobRequestType.GET, blob );
            mockDaoDriver.updateBean(
                    chunk.setReadFromDs3TargetId( target.getId() ),
                    ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        final ReadChunkFromDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager()).buildReadChunkFromDs3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.values()[0],
                        target.getId(),
                        PersistenceType.DS3,
                        List.of(chunk)
                ) );

        dbSupport.getServiceManager().getService( JobEntryService.class ).delete( chunk.getId() );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed to not retry.");
    }


    @Test
    public void testRunWhenJobChunkHasNoEntriesDoesNothing()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromDs3TargetId( target.getId() ),
                    ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildReadChunkFromDs3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.values()[0],
                        target.getId(),
                        PersistenceType.DS3,
                        new ArrayList<>(chunks)
                ) );
        cacheManager.blobLoadedToCache( b1.getId() );
        cacheManager.blobLoadedToCache( b2.getId() );

        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
    }


    @Test
    public void testRunWhenJobChunkReadyToGetWorks()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDs3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory();
        ds3ConnectionFactory.setGetChunkReadyToReadResponse( Boolean.TRUE );
        ds3ConnectionFactory.setGetBlobException( null );
        ds3ConnectionFactory.setIsJobExistantResponse( false );
        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setId( UUID.randomUUID() );
        ds3ConnectionFactory.setCreateGetJobResponse( remoteJob.getJob() );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        ds3ConnectionFactory.setBlobsReady( CollectionFactory.toList( b1.getId(), b2.getId() ) );



        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromDs3TargetId( target.getId() ),
                    ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withDs3ConnectionFactory(ds3ConnectionFactory)
                .buildReadChunkFromDs3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.values()[0],
                        target.getId(),
                        PersistenceType.DS3,
                        new ArrayList<>(chunks)
                ) );
        cacheManager.blobLoadedToCache( b1.getId() );

        task.prepareForExecutionIfPossible();

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain( chunk ).getBlobStoreState(), "Shoulda updated job chunk blob store state for success.");
        }
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda loaded blob into cache due to success.");
    }


    @Test
    public void testRunWhenJobChunkNotReadyToGetWaitsUntilReady()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDs3ConnectionFactory ds3ConnectionFactory = new MockDs3ConnectionFactory();
        ds3ConnectionFactory.setGetChunkReadyToReadResponse( Boolean.FALSE );
        ds3ConnectionFactory.setIsJobExistantResponse( false );
        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setId( UUID.randomUUID() );
        ds3ConnectionFactory.setCreateGetJobResponse( remoteJob.getJob() );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromDs3TargetId( target.getId() ),
                    ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDs3ConnectionFactory(ds3ConnectionFactory)
                .withDiskManager(cacheManager)
                .buildReadChunkFromDs3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.values()[0],
                        target.getId(),
                        PersistenceType.DS3,
                        new ArrayList<>(chunks)
                ) );
        cacheManager.blobLoadedToCache( b1.getId() );

        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended.");
        for (JobEntry chunk : chunks) {
            if (chunk.getBlobId() == b1.getId()) {
                assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain(chunk).getBlobStoreState(), "Should notta updated job chunk blob store state yet.");
            } else {
                assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.attain(chunk).getBlobStoreState(), "Should notta updated job chunk blob store state yet.");
            }
        }
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated failure.");

        ds3ConnectionFactory.setGetChunkReadyToReadResponse( Boolean.TRUE );
        //NOTE: only b2, because b1 is in cache
        ds3ConnectionFactory.setBlobsReady( CollectionFactory.toList( b2.getId() ) );

        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( "Get blob request should've failed", RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported suspended.");
        for (JobEntry chunk : chunks) {
            if (chunk.getBlobId() == b1.getId()) {
                assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain(chunk).getBlobStoreState(), "Should notta updated job chunk blob store state yet.");
            } else {
                assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.attain(chunk).getBlobStoreState(), "Should notta updated job chunk blob store state yet.");
            }
        }
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Shoulda generated failure.");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertFalse(cacheManager.isOnDisk( b2.getId() ), "Should notta loaded blob into cache due to failure.");

        ds3ConnectionFactory.setGetBlobException( null );
        
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain(chunk).getBlobStoreState(), "Shoulda updated job chunk blob store state due to success.");
        }
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(Ds3TargetFailure.class).getCount(), "Should notta generated failure.");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda loaded blob into cache due to success.");
    }


    /**
     * Tests blob-level concurrency prevention across different job chunks.
     */
    @Test
    public void testRunPreventsConcurrentReadingOfSameBlobs()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDs3ConnectionFactory ds3ConnectionFactory1 = new MockDs3ConnectionFactory();
        final MockDs3ConnectionFactory ds3ConnectionFactory2 = new MockDs3ConnectionFactory();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        final Ds3Target target = mockDaoDriver.createDs3Target( "t1" );
        final Set<JobEntry> chunk1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        final Set<JobEntry> chunk2 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b3 ) );
        for (JobEntry chunk : chunk1) {
            mockDaoDriver.updateBean( chunk.setReadFromDs3TargetId( target.getId() ), ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        }
        for (JobEntry chunk : chunk2) {
            mockDaoDriver.updateBean( chunk.setReadFromDs3TargetId( target.getId() ), ReadFromObservable.READ_FROM_DS3_TARGET_ID );
        }

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );

        final ReadChunkFromDs3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withDs3ConnectionFactory(ds3ConnectionFactory1)
                .buildReadChunkFromDs3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.DS3,
                        chunk1.stream().toList() ));

        final ReadChunkFromDs3TargetTask task2 = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withDs3ConnectionFactory(ds3ConnectionFactory2)
                .buildReadChunkFromDs3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.DS3,
                        chunk2.stream().toList() ));

        final DetailedJobToReplicate remoteJob = BeanFactory.newBean( DetailedJobToReplicate.class );
        remoteJob.setJob( BeanFactory.newBean( JobToReplicate.class ) );
        remoteJob.getJob().setId( UUID.randomUUID() );
        ds3ConnectionFactory1.setGetChunkReadyToReadResponse( Boolean.TRUE );
        ds3ConnectionFactory1.setBlobsReady( CollectionFactory.toList( b1.getId(), b2.getId() ) );
        ds3ConnectionFactory1.setGetBlobException( null );
        ds3ConnectionFactory1.setIsJobExistantResponse( false );
        ds3ConnectionFactory1.setCreateGetJobResponse( remoteJob.getJob() );
        ds3ConnectionFactory2.setGetChunkReadyToReadResponse( Boolean.TRUE );
        ds3ConnectionFactory2.setBlobsReady( CollectionFactory.toList( b1.getId(), b3.getId() ) );
        ds3ConnectionFactory2.setGetBlobException( null );
        ds3ConnectionFactory2.setIsJobExistantResponse( false );
        ds3ConnectionFactory2.setCreateGetJobResponse( remoteJob.getJob() );
        
        ds3ConnectionFactory1.setInvocationHandler( new InvocationHandler()
        {
            private volatile boolean task2Submitted = false;
            
            public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
            {
                if ( "getBlob".equals( method.getName() ) )
                {
                    if ( !task2Submitted )
                    {
                        task2Submitted = true;
                        SystemWorkPool.getInstance().submit( task2 );
                        Thread.sleep( 100 );
                        assertEquals(BlobStoreTaskState.NOT_READY, task2.getState(), "Task2 should be suspended while task1 is reading blob b1");
                    }
                    throw new RuntimeException( "Simulated task1 failure" );
                }
                return method.invoke( ds3ConnectionFactory1, args );
            }
        } );
        
        ds3ConnectionFactory2.setInvocationHandler( new InvocationHandler()
        {
            public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
            {
                if ( "getBlob".equals( method.getName() ) )
                {
                    cacheManager.blobLoadedToCache( b1.getId() );
                    cacheManager.blobLoadedToCache( b3.getId() );
                    return null;
                }
                return method.invoke( ds3ConnectionFactory2, args );
            }
        } );

        task.prepareForExecutionIfPossible();
        task2.prepareForExecutionIfPossible();

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        
        TestUtil.sleep( 200 );
        task2.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task2 );

        assertEquals(BlobStoreTaskState.COMPLETED, task2.getState(), "Task2 should succeed after task1 releases locks");
        for (JobEntry chunk : chunk2) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain( chunk ).getBlobStoreState(), "Chunk2 should be completed");
        }
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Blob b1 should be cached");
        assertTrue(cacheManager.isOnDisk( b3.getId() ), "Blob b3 should be cached");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
