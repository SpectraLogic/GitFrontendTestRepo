/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.rpc.target.*;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.testfrmwrk.TargetTaskBuilder;

import com.spectralogic.util.db.query.Require;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public final class ReadChunkFromS3TargetTask_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
             public void test()
            {
                new TargetTaskBuilder(null).buildReadChunkFromS3TargetTask(
                        new ReadDirective(
                                BlobStoreTaskPriority.values()[0],
                                UUID.randomUUID(),
                                PersistenceType.S3,
                                new ArrayList<>()
                        ));
            }
        } );
    }


    @Test
    public void testHappyConstruction()
    {
        new TargetTaskBuilder(dbSupport.getServiceManager()).buildReadChunkFromS3TargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        UUID.randomUUID(),
                        PersistenceType.S3,
                        new ArrayList()
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

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
        mockDaoDriver.updateBean(
                chunk.setReadFromS3TargetId(target.getId()),
                ReadFromObservable.READ_FROM_S3_TARGET_ID);
        }
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
            .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                new ArrayList(chunks)
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

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                chunk.setReadFromS3TargetId( target.getId() ),
                ReadFromObservable.READ_FROM_S3_TARGET_ID );
        }
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
            .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                new ArrayList(chunks)
            ));

        for (JobEntry chunk : chunks) {
        dbSupport.getServiceManager().getService( JobEntryService.class ).delete( chunk.getId() );
        }
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

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromS3TargetId(target.getId()),
                    ReadFromObservable.READ_FROM_S3_TARGET_ID);
        }
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
            .withDiskManager(cacheManager)
            .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                new ArrayList(chunks)
            ));
        cacheManager.blobLoadedToCache( b1.getId() );
        cacheManager.blobLoadedToCache( b2.getId() );

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
    }


     @Test
    public void testRunWhenBucketDoesNotExistFails()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final S3ConnectionFactory s3ConnectionFactory = mock( S3ConnectionFactory.class );
        final S3Connection connection = mock( S3Connection.class );
        when( s3ConnectionFactory.connect( any() ) ).thenReturn( connection );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromS3TargetId(target.getId()),
                    ReadFromObservable.READ_FROM_S3_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
            .withDiskManager(cacheManager)
            .withS3ConnectionFactory(s3ConnectionFactory)
            .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                new ArrayList(chunks)
            ));

        cacheManager.blobLoadedToCache( b1.getId() );

        when( connection.getExistingBucketInformation( any() ) ).thenReturn( null );
        when( connection.isBlobAvailableOnCloud( any(), any(), any() ) ).thenReturn( true );
        when( connection.isBlobReadyToBeReadFromCloud( any(), any(), any() ) ).thenReturn( true );

        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "does not exist" ), "Failure message shoulda been helpful.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( UUID.randomUUID() ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {

            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "owned by another appliance" ), "Failure message shoulda been helpful.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 33 ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "this version of software only supports" ), "Failure message shoulda been helpful.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( "invalid" )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() ) );
        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "the cloud bucket to be for" ), "Failure message shoulda been helpful.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(S3TargetFailure.class).getCount(), "Shoulda generated target failure.");

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );
        final Future< ? > failedFuture = mock( Future.class );


        when( connection.readBlobFromCloud( any(), any(), any(), any() ) )
                .thenReturn( List.of( failedFuture) );
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain( chunk ).getBlobStoreState(), "Shoulda updated job chunk blob store state for success.");
        }
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda loaded blob into cache due to success.");
    }

    /*
    One blob is offline and other is missing. Both blobs should be in PENDING state and task suspended.
     */
    @Test
    public void testRunSuspectOfflineBlobsS3() throws Exception {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final S3ConnectionFactory s3ConnectionFactory = mock( S3ConnectionFactory.class );
        final S3Connection connection = mock( S3Connection.class );

        when( s3ConnectionFactory.connect( any() ) ).thenReturn( connection );

        final Future< ? > failedFuture1 = mock( Future.class );
        final Future< ? > failedFuture2 = mock( Future.class );


        when( connection.readBlobFromCloud( any(), any(), any(), any() ) )
                .thenReturn( List.of( failedFuture1 ) )
                .thenReturn( List.of( failedFuture2 ) );

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromS3TargetId(target.getId()),
                    ReadFromObservable.READ_FROM_S3_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final MockOfflineDataStagingWindowManager stagingManager =
                new MockOfflineDataStagingWindowManager();
        stagingManager.setTryLockReturnValue( false );

        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory)
                .withOfflineStagingWindow(stagingManager)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        new ArrayList(chunks)
                ));

        final BlobS3Target blobS3Target = BeanFactory.newBean( BlobS3Target.class );
        blobS3Target.setBlobId( b1.getId() );
        blobS3Target.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target );
        mockDaoDriver.makeSuspect( blobS3Target );

        final BlobS3Target blobS3Target2 = BeanFactory.newBean( BlobS3Target.class );
        blobS3Target2.setBlobId( b2.getId() );
        blobS3Target2.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target2 );

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );
        when( connection.isBlobReadyToBeReadFromCloud( any(), any(), any() ) )
                .thenReturn( false )
                .thenReturn( false );

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );


        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported not ready.");
        assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.getJobEntryFor( b1.getId() ).getBlobStoreState(), "Shoulda reported pending.");
        assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.getJobEntryFor( b2.getId() ).getBlobStoreState(), "Shoulda reported pending.");
    }

    /*
    One blob is missing. Missing blob should be in PENDING state and task completed.
     */
    @Test
    public void testRunSuspectBlobsS3() throws Exception {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final S3ConnectionFactory s3ConnectionFactory = mock( S3ConnectionFactory.class );
        final S3Connection connection = mock( S3Connection.class );

        when( s3ConnectionFactory.connect( any() ) ).thenReturn( connection );

        final Future< ? > failedFuture1 = mock( Future.class );
        final Future< ? > failedFuture2 = mock( Future.class );


        when( connection.readBlobFromCloud( any(), any(), any(), any() ) )
                .thenReturn( List.of( failedFuture1 ) )
                .thenReturn( List.of( failedFuture2 ) );

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromS3TargetId(target.getId()),
                    ReadFromObservable.READ_FROM_S3_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final MockOfflineDataStagingWindowManager stagingManager =
                new MockOfflineDataStagingWindowManager();
        stagingManager.setTryLockReturnValue( false );

        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory)
               // .withOfflineStagingWindow(stagingManager)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        new ArrayList(chunks)
                ));

        final BlobS3Target blobS3Target = BeanFactory.newBean( BlobS3Target.class );
        blobS3Target.setBlobId( b1.getId() );
        blobS3Target.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target );
        mockDaoDriver.makeSuspect( blobS3Target );

        final BlobS3Target blobS3Target2 = BeanFactory.newBean( BlobS3Target.class );
        blobS3Target2.setBlobId( b2.getId() );
        blobS3Target2.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target2 );


        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );
        when( connection.isBlobAvailableOnCloud( any(), any(), argThat(  blob -> blob != null && b1.getId().equals( blob.getId() ) ) ) )
                .thenReturn( false );

        when( connection.isBlobAvailableOnCloud( any(), any(), argThat(  blob -> blob != null && b2.getId().equals( blob.getId() ) ) ) )
                .thenReturn( true );

        when( connection.isBlobReadyToBeReadFromCloud( any(), any(), any() ))
                .thenReturn( true );


        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        TestUtil.sleep( 500 );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported not ready.");
        assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.getJobEntryFor( b1.getId() ).getBlobStoreState(), "Shoulda reported pending.");
        assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.getJobEntryFor( b2.getId() ).getBlobStoreState(), "Shoulda reported pending.");
    }

    @Test
    public void testReadFailuresS3() throws Exception {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final S3ConnectionFactory s3ConnectionFactory = mock( S3ConnectionFactory.class );
        final S3Connection connection = mock( S3Connection.class );

        when( s3ConnectionFactory.connect( any() ) ).thenReturn( connection );

        final Future< ? > failedFuture1 = mock( Future.class );
        final Future< ? > failedFuture2 = mock( Future.class );

        final S3Exception s3Exception = (S3Exception) S3Exception.builder().statusCode( 404 ).message( "Simulated Failure" ).build();
        final S3Exception s3Exception2 = (S3Exception) S3Exception.builder().statusCode( 505 ).message( "Simulated Failure" ).build();
        when( failedFuture1.get() ).thenThrow( new ExecutionException( s3Exception ) );
        when( failedFuture2.get() ).thenThrow( new ExecutionException( s3Exception2 ) );

        when( connection.readBlobFromCloud( any(), any(), any(), any() ) )
                .thenReturn( List.of( failedFuture1 ) )
                .thenReturn( List.of( failedFuture2 ) );

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromS3TargetId(target.getId()),
                    ReadFromObservable.READ_FROM_S3_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        new ArrayList(chunks)
                ));

        final BlobS3Target blobS3Target = BeanFactory.newBean( BlobS3Target.class );
        blobS3Target.setBlobId( b1.getId() );
        blobS3Target.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target );

        final BlobS3Target blobS3Target2 = BeanFactory.newBean( BlobS3Target.class );
        blobS3Target2.setBlobId( b2.getId() );
        blobS3Target2.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobS3Target.class ).create( blobS3Target2 );

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );

        when( connection.isBlobAvailableOnCloud( any(), any(), any() ) ).thenReturn( true );
        when( connection.isBlobReadyToBeReadFromCloud( any(), any(), any() ) ).thenReturn( true );


        task.prepareForExecutionIfPossible();

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );


        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported completed.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.attain( chunk ).getBlobStoreState(), "Shoulda updated job chunk blob store state for success.");
        }
        
    }


     @Test
    public void testRunPerformsGetsCorrectlyWhenAllDataOnline()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final S3ConnectionFactory s3ConnectionFactory = mock( S3ConnectionFactory.class );
        final S3Connection connection = mock( S3Connection.class );

        when( s3ConnectionFactory.connect( any() ) ).thenReturn( connection );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final MockOfflineDataStagingWindowManager stagingManager =
                new MockOfflineDataStagingWindowManager();
        stagingManager.setTryLockReturnValue( false );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final JobEntry entry1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1 ) )
                .iterator().next()
                .setReadFromS3TargetId( target.getId() );
        final JobEntry entry2 = mockDaoDriver.createJobEntry(entry1.getJobId(), b2)
                .setReadFromS3TargetId( target.getId() );
        mockDaoDriver.updateBean(entry1, ReadFromObservable.READ_FROM_S3_TARGET_ID);
        mockDaoDriver.updateBean(entry2, ReadFromObservable.READ_FROM_S3_TARGET_ID);

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory)
                .withOfflineStagingWindow(stagingManager)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        List.of(entry1, entry2)
                ));

        cacheManager.blobLoadedToCache( b1.getId() );

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );

        when( connection.isBlobAvailableOnCloud( any(), any(), any() ) ).thenReturn( true );
        when( connection.isBlobReadyToBeReadFromCloud( any(), any(), any() ) ).thenReturn( true );
        final Future< ? > future1 = mock( Future.class );
        final Future< ? > future2 = mock( Future.class );


        when( connection.readBlobFromCloud( any(), any(), any(), any() ) )
                .thenReturn( List.of( future1, future2 ) );
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
        assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain(entry1).getBlobStoreState(), "Shoulda updated job chunk blob store state for success.");
        assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain(entry2).getBlobStoreState(), "Shoulda updated job chunk blob store state for success.");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda loaded blob into cache due to success.");
        stagingManager.assertTryLockCalls();
        stagingManager.assertReleaseLockCalls(entry2.getId());
    }


     @Test
    public void testRunDeferredWhenAllDataOfflineCannotAcquireStageLock()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final S3ConnectionFactory s3ConnectionFactory = mock( S3ConnectionFactory.class );
        final S3Connection connection = mock( S3Connection.class );

        when( s3ConnectionFactory.connect( any() ) ).thenReturn( connection );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final MockOfflineDataStagingWindowManager stagingManager =
                new MockOfflineDataStagingWindowManager();
        stagingManager.setTryLockReturnValue( false );
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final JobEntry entry1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1 ) )
                .iterator().next()
                .setReadFromS3TargetId( target.getId() );
        final JobEntry entry2 = mockDaoDriver.createJobEntry(entry1.getJobId(), b2)
                .setReadFromS3TargetId( target.getId() );
        mockDaoDriver.updateBean(entry1, ReadFromObservable.READ_FROM_S3_TARGET_ID);
        mockDaoDriver.updateBean(entry2, ReadFromObservable.READ_FROM_S3_TARGET_ID);

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory)
                .withOfflineStagingWindow(stagingManager)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                List.of(entry1, entry2)
        ));

        cacheManager.blobLoadedToCache( b1.getId() );

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );

        when( connection.isBlobAvailableOnCloud( any(), any(), any() ) ).thenReturn( true );
        when( connection.isBlobReadyToBeReadFromCloud( any(), any(), any() ) ).thenReturn( false );


        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported needs to run again.");
        assertTrue(mockDaoDriver.getServiceManager().getRetriever(JobEntry.class)
                        .all(Require.beanPropertyEquals(JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "All entries should need to run again");
        stagingManager.assertTryLockCalls(b2.getId());
        stagingManager.assertReleaseLockCalls();
    }


     @Test
    public void testRunDeferredWhenAllDataOfflineCanAcquireStageLock()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        S3ConnectionFactory s3ConnectionFactory = mock(S3ConnectionFactory.class);
        S3Connection connection = mock(S3Connection.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final MockOfflineDataStagingWindowManager stagingManager =
                new MockOfflineDataStagingWindowManager();
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final JobEntry entry1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1 ) )
                .iterator().next()
                .setReadFromS3TargetId( target.getId() );
        final JobEntry entry2 = mockDaoDriver.createJobEntry(entry1.getJobId(), b2)
                .setReadFromS3TargetId( target.getId() );
        mockDaoDriver.updateBean(entry1, ReadFromObservable.READ_FROM_S3_TARGET_ID);
        mockDaoDriver.updateBean(entry2, ReadFromObservable.READ_FROM_S3_TARGET_ID);

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withS3ConnectionFactory(s3ConnectionFactory)
                .withDiskManager(cacheManager)
                .withOfflineStagingWindow(stagingManager)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                List.of(entry1, entry2)
        ));

        cacheManager.blobLoadedToCache( b1.getId() );

        when(s3ConnectionFactory.connect(any())).thenReturn(connection);


        PublicCloudBucketInformation bucketInfo = BeanFactory.newBean(PublicCloudBucketInformation.class)
                .setLocalBucketName(bucket.getName())
                .setOwnerId(mockDaoDriver.attainOneAndOnly(DataPathBackend.class).getInstanceId())
                .setVersion(1);

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);
        when(connection.isBlobAvailableOnCloud(any(), any(), any()))
                .thenReturn(true);


        when(connection.isBlobReadyToBeReadFromCloud(any(), any(), any()))
                .thenReturn(false);

        final Future< ? > future1 = mock( Future.class );
        final Future< ? > future2 = mock( Future.class );
        when(connection.readBlobFromCloud(any(), any(), any(), any()))
                .thenReturn(List.of(future1, future2));


        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported needs to run again.");
        assertTrue(mockDaoDriver.getServiceManager().getRetriever(JobEntry.class)
                        .all(Require.beanPropertyEquals(JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "All entries should have been pending");
        stagingManager.assertTryLockCalls(b2.getId());
        stagingManager.assertReleaseLockCalls();

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported needs to run again.");
        assertTrue(mockDaoDriver.getServiceManager().getRetriever(JobEntry.class)
                        .all(Require.beanPropertyEquals(JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "All entries should have been pending");
        stagingManager.assertTryLockCalls(b2.getId());
        stagingManager.assertReleaseLockCalls();

        when(connection.isBlobAvailableOnCloud(any(), any(), any())).thenReturn(true);

        when(connection.isBlobReadyToBeReadFromCloud(any(), any(), any())).thenReturn(true);




        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported complete.");
        stagingManager.assertTryLockCalls();
        stagingManager.assertReleaseLockCalls(entry2.getId());
    }


     @Test
    public void testRunDeferredWhenSomeDataOfflineCanAcquireStageLock()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        S3ConnectionFactory s3ConnectionFactory = mock(S3ConnectionFactory.class);
        S3Connection connection = mock(S3Connection.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        final MockOfflineDataStagingWindowManager stagingManager =
                new MockOfflineDataStagingWindowManager();
        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final JobEntry entry1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1 ) )
                .iterator().next()
                .setReadFromS3TargetId( target.getId() );
        final JobEntry entry2 = mockDaoDriver.createJobEntry(entry1.getJobId(), b2)
                .setReadFromS3TargetId( target.getId() );
        final JobEntry entry3 = mockDaoDriver.createJobEntry(entry1.getJobId(), b3)
                .setReadFromS3TargetId( target.getId() );
        mockDaoDriver.updateBean(entry1, ReadFromObservable.READ_FROM_S3_TARGET_ID);
        mockDaoDriver.updateBean(entry2, ReadFromObservable.READ_FROM_S3_TARGET_ID);
        mockDaoDriver.updateBean(entry3, ReadFromObservable.READ_FROM_S3_TARGET_ID);

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory)
                .withOfflineStagingWindow(stagingManager)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                BlobStoreTaskPriority.NORMAL,
                target.getId(),
                PersistenceType.S3,
                List.of(entry1, entry2, entry3)
        ));

        cacheManager.blobLoadedToCache( b3.getId() );


        when(s3ConnectionFactory.connect(any())).thenReturn(connection);


        PublicCloudBucketInformation bucketInfo = BeanFactory.newBean(PublicCloudBucketInformation.class)
                .setLocalBucketName(bucket.getName())
                .setOwnerId(mockDaoDriver.attainOneAndOnly(DataPathBackend.class).getInstanceId())
                .setVersion(1);

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);
        when(connection.isBlobAvailableOnCloud(any(), any(), any())).thenAnswer(invocation -> {
            return true;
        });

        when(connection.isBlobReadyToBeReadFromCloud(any(), any(), any())).thenAnswer(invocation -> {
            Blob blob = invocation.getArgument(2);
            if (blob == null) return false;
            return Boolean.valueOf(b1.getId().equals(blob.getId()));
        });

        final Future< ? > failedFuture1 = mock( Future.class );
        final Future< ? > failedFuture2 = mock( Future.class );
        when(connection.readBlobFromCloud(any(), any(), any(), any())).thenAnswer(invocation ->
                List.of(failedFuture1, failedFuture2)
        );
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported needs to run again.");
        assertTrue(mockDaoDriver.getServiceManager().getRetriever(JobEntry.class)
                        .all(Require.beanPropertyEquals(JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "All entries should have been pending");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda reported b1 as being loaded into cache.");
        assertFalse(cacheManager.isOnDisk(b2.getId()), "Should notta reported b2 as being loaded into cache.");
        stagingManager.assertTryLockCalls(b2.getId());
        stagingManager.assertReleaseLockCalls(entry1.getId());

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported needs to run again.");
        assertTrue(mockDaoDriver.getServiceManager().getRetriever(JobEntry.class)
                        .all(Require.beanPropertyEquals(JobEntry.BLOB_STORE_STATE, JobChunkBlobStoreState.PENDING)), "All entries should have been pending");
        stagingManager.assertTryLockCalls(b2.getId());
        stagingManager.assertReleaseLockCalls();

         bucketInfo =  BeanFactory.newBean( PublicCloudBucketInformation.class )
                 .setLocalBucketName( bucket.getName() )
                 .setOwnerId( mockDaoDriver.attainOneAndOnly(
                         DataPathBackend.class ).getInstanceId() )
                 .setVersion( 1 );

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);
        when(connection.isBlobAvailableOnCloud(any(), any(), any())).thenAnswer(invocation -> {
            return true;
        });

        when(connection.isBlobReadyToBeReadFromCloud(any(), any(), any())).thenAnswer(invocation -> {
            Blob blob = invocation.getArgument(2);
            if ( !b2.getId().equals( blob.getId() ) )
            {
                throw new RuntimeException(
                        "Why try to determine if a blob is offline that's in cache?" );
            }
            return Boolean.TRUE;
        });



        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported complete.");
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda reported b1 as being loaded into cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda reported b2 as being loaded into cache.");
        stagingManager.assertTryLockCalls();
        stagingManager.assertReleaseLockCalls(entry2.getId());
    }


     @Test
    public void testRunPreventsConcurrentReadingOfSameBlobs()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        S3ConnectionFactory s3ConnectionFactory1 = mock(S3ConnectionFactory.class);
        S3ConnectionFactory s3ConnectionFactory2 = mock(S3ConnectionFactory.class);
        final S3Connection connection1 = mock(S3Connection.class);
        when(s3ConnectionFactory1.connect(any())).thenReturn(connection1);
        final S3Connection connection2 = mock(S3Connection.class);
        when(s3ConnectionFactory2.connect(any())).thenReturn(connection2);

       /* final MockS3ConnectionFactory s3ConnectionFactory1 = new MockS3ConnectionFactory();
        s3ConnectionFactory1.setConnectException( null );
        final MockS3ConnectionFactory s3ConnectionFactory2 = new MockS3ConnectionFactory();
        s3ConnectionFactory2.setConnectException( null );*/

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        final S3Target target = mockDaoDriver.createS3Target( "t1" );
        final Set<JobEntry> chunk1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        final Set<JobEntry> chunk2 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b3 ) );
        for (JobEntry chunk : chunk1) {
            mockDaoDriver.updateBean( chunk.setReadFromS3TargetId( target.getId() ), ReadFromObservable.READ_FROM_S3_TARGET_ID );
        }
        for (JobEntry chunk : chunk2) {
            mockDaoDriver.updateBean( chunk.setReadFromS3TargetId( target.getId() ), ReadFromObservable.READ_FROM_S3_TARGET_ID );
        }

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );

        final ReadChunkFromS3TargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory1)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        chunk1.stream().toList() ));

        final ReadChunkFromS3TargetTask task2 = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withS3ConnectionFactory(s3ConnectionFactory2)
                .buildReadChunkFromS3TargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        chunk2.stream().toList() ));

        PublicCloudBucketInformation bucketInfo = BeanFactory.newBean(PublicCloudBucketInformation.class)
                .setLocalBucketName(bucket.getName())
                .setOwnerId(mockDaoDriver.attainOneAndOnly(DataPathBackend.class).getInstanceId())
                .setVersion(1);

        when(connection1.getExistingBucketInformation(any())).thenReturn(bucketInfo);
        when(connection1.isBlobAvailableOnCloud(any(), any(), any())).thenAnswer(invocation -> {
            return true;
        });

        when(connection1.isBlobReadyToBeReadFromCloud(any(), any(), any())).thenAnswer(invocation -> {
           return true;
        });

        final AtomicBoolean task2Submitted = new AtomicBoolean(false);
        when(connection1.readBlobFromCloud(any(), any(), any(), any())).thenAnswer(invocation -> {
            // Use an AtomicBoolean to ensure the block runs only once
            if (!task2Submitted.getAndSet(true)) {
                SystemWorkPool.getInstance().submit(task2);
                Thread.sleep(100);
                assertEquals(BlobStoreTaskState.READY, task2.getState(), "Task2 should be blocked while task1 is reading blob b1");
            }
            // Always throw the simulated failure after the logic runs
            throw new RuntimeException("Simulated task1 failure");
        });

        when(connection2.getExistingBucketInformation(any())).thenReturn(bucketInfo);
        when(connection2.isBlobAvailableOnCloud(any(), any(), any())).thenAnswer(invocation -> {
            return true;
        });

        when(connection2.isBlobReadyToBeReadFromCloud(any(), any(), any())).thenAnswer(invocation -> {
            return true;
        });
        final Future< ? > failedFuture1 = mock( Future.class );
        final Future< ? > failedFuture2 = mock( Future.class );
        when(connection2.readBlobFromCloud(any(), any(), any(), any())).thenAnswer(invocation -> {
            cacheManager.blobLoadedToCache(b1.getId());
            cacheManager.blobLoadedToCache(b3.getId());
            return  List.of(failedFuture1, failedFuture2); // Return null as per the original InvocationHandler
        });


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
