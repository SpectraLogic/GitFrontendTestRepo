/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;


import com.spectralogic.s3.common.rpc.target.*;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.testfrmwrk.TargetTaskBuilder;
import com.spectralogic.util.exception.BlobReadFailedException;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.mock.InterfaceProxyFactory;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

import java.lang.reflect.InvocationHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.mockito.stubbing.Answer;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ReadChunkFromAzureTargetTask_Test
{

    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TargetTaskBuilder(null).buildReadChunkFromAzureTargetTask(
                   new ReadDirective(
                           BlobStoreTaskPriority.values()[0],
                           UUID.randomUUID(),
                           PersistenceType.AZURE,
                           new ArrayList<>()
                   ));
            }
        } );
    }


     @Test
    public void testHappyConstruction()
    {
        new TargetTaskBuilder(dbSupport.getServiceManager()).buildReadChunkFromAzureTargetTask(
            new ReadDirective(
                    BlobStoreTaskPriority.NORMAL,
                    UUID.randomUUID(),
                    PersistenceType.AZURE,
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

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromAzureTargetId(target.getId()),
                    ReadFromObservable.READ_FROM_AZURE_TARGET_ID);
        }
        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        new ArrayList(chunks)
                ));

        final Object expected = chunks.iterator().next();
        assertEquals(expected,  task.getEntries().iterator().next(), "Shoulda returned chunk.");
    }


     @Test
    public void testPrepareForExecutionWhenJobChunkNoLongerExistsNotPossible()
    {

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( blob ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromAzureTargetId( target.getId() ),
                    ReadFromObservable.READ_FROM_AZURE_TARGET_ID );
        }
        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        new ArrayList(chunks)
                ));

        for (JobEntry chunk : chunks) {
            dbSupport.getServiceManager().getService( JobEntryService.class ).delete( chunk.getId() );
        }
        task.prepareForExecutionIfPossible();
        final Object actual = task.getState();
        assertEquals((Object) BlobStoreTaskState.COMPLETED, actual, "Shoulda reported completed to not retry.");
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

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromAzureTargetId(target.getId()),
                    ReadFromObservable.READ_FROM_AZURE_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        new ArrayList(chunks)
                ));
        cacheManager.blobLoadedToCache( b1.getId() );
        cacheManager.blobLoadedToCache( b2.getId() );

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual = task.getState();
        assertEquals((Object) BlobStoreTaskState.COMPLETED, actual, "Shoulda reported completed.");
    }


     @Test
    public void testRunWhenBucketDoesNotExistFails()
    {

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        AzureConnectionFactory azureConnectionFactory = mock(AzureConnectionFactory.class);
        AzureConnection connection = mock(AzureConnection.class);

        when( azureConnectionFactory.connect( any() ) ).thenReturn( connection );


        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromAzureTargetId(target.getId()),
                    ReadFromObservable.READ_FROM_AZURE_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnectionFactory).buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        new ArrayList(chunks)
                ));
        cacheManager.blobLoadedToCache( b1.getId() );



        when(connection.getExistingBucketInformation(any())).thenReturn(null);

        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "does not exist" ), "Failure message shoulda been helpful.");
        final Object actual5 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 1, actual5, "Shoulda generated target failure.");

        PublicCloudBucketInformation bucketInfo = BeanFactory.newBean(PublicCloudBucketInformation.class)
                .setLocalBucketName(bucket.getName())
                .setOwnerId(UUID.randomUUID())
                .setVersion(1);

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);


        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    TestUtil.invokeAndWaitChecked( task );
                }
        } ).getMessage().contains( "owned by another appliance" ), "Failure message shoulda been helpful.");
        final Object actual4 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 2, actual4, "Shoulda generated target failure.");

       bucketInfo = BeanFactory.newBean(PublicCloudBucketInformation.class)
                .setLocalBucketName(bucket.getName())
                .setOwnerId( mockDaoDriver.attainOneAndOnly(
                       DataPathBackend.class ).getInstanceId() )
                .setVersion( 33 );

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);


        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "this version of software only supports" ), "Failure message shoulda been helpful.");
        final Object actual3 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 3, actual3, "Shoulda generated target failure.");

        bucketInfo = BeanFactory.newBean( PublicCloudBucketInformation.class )
                .setLocalBucketName( "invalid" )
                .setOwnerId( mockDaoDriver.attainOneAndOnly(
                        DataPathBackend.class ).getInstanceId() ) ;

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);


        task.prepareForExecutionIfPossible();
        assertTrue(TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } ).getMessage().contains( "the cloud bucket to be for" ), "Failure message shoulda been helpful.");
        final Object actual2 = dbSupport.getServiceManager().getRetriever( AzureTargetFailure.class ).getCount();
        assertEquals((Object) 4, actual2, "Shoulda generated target failure.");

        bucketInfo = BeanFactory.newBean( PublicCloudBucketInformation.class )
                .setLocalBucketName( bucket.getName() )
                .setOwnerId( mockDaoDriver.attainOneAndOnly(
                        DataPathBackend.class ).getInstanceId() )
                .setVersion( 1 )  ;

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);
        final Future< ? > future1 = mock( Future.class );
        final Future< ? > future2 = mock( Future.class );
        when(connection.readBlobFromCloud(any(), any(), any(), any()))
                .thenReturn(List.of(future1, future2));

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual1 = task.getState();
        assertEquals((Object) BlobStoreTaskState.COMPLETED, actual1, "Shoulda reported completed.");
        for (JobEntry chunk : chunks) {
            final Object actual = mockDaoDriver.attain( chunk ).getBlobStoreState();
            assertEquals((Object) JobChunkBlobStoreState.COMPLETED, actual, "Shoulda updated job chunk blob store state for success.");
        }
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda loaded blob into cache due to success.");
    }


     @Test
    public void testRunPerformsGetsCorrectly()
    {

        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        AzureConnectionFactory azureConnectionFactory = mock(AzureConnectionFactory.class);
        AzureConnection connection = mock(AzureConnection.class);

        when( azureConnectionFactory.connect( any() ) ).thenReturn( connection );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromAzureTargetId(target.getId()),
                    ReadFromObservable.READ_FROM_AZURE_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnectionFactory).buildReadChunkFromAzureTargetTask(
                new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        new ArrayList(chunks)
                ));
        cacheManager.blobLoadedToCache( b1.getId() );

        PublicCloudBucketInformation bucketInfo =BeanFactory.newBean( PublicCloudBucketInformation.class )
                .setLocalBucketName( bucket.getName() )
                .setOwnerId( mockDaoDriver.attainOneAndOnly(
                        DataPathBackend.class ).getInstanceId() )
                .setVersion( 1 );

        when(connection.getExistingBucketInformation(any())).thenReturn(bucketInfo);

        final Future< ? > future1 = mock( Future.class );
        final Future< ? > future2 = mock( Future.class );
        when(connection.readBlobFromCloud(any(), any(), any(), any()))
                .thenReturn(List.of(future1, future2));
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual1 = task.getState();
        assertEquals((Object) BlobStoreTaskState.COMPLETED, actual1, "Shoulda reported completed.");
        for (JobEntry chunk : chunks) {
            final Object actual = mockDaoDriver.attain(chunk).getBlobStoreState();
            assertEquals((Object) JobChunkBlobStoreState.COMPLETED, actual, "Shoulda updated job chunk blob store state for success.");
        }
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Shoulda notta whacked blob from cache.");
        assertTrue(cacheManager.isOnDisk( b2.getId() ), "Shoulda loaded blob into cache due to success.");
    }


     @Test
    public void testRunPreventsConcurrentReadingOfSameBlobs()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final AzureConnectionFactory azureConnectionFactory1 = mock( AzureConnectionFactory.class );
        final AzureConnection connection1 = mock( AzureConnection.class );
        when( azureConnectionFactory1.connect( any() ) ).thenReturn( connection1 );

        final AzureConnectionFactory azureConnectionFactory2 = mock( AzureConnectionFactory.class );
        final AzureConnection connection2 = mock( AzureConnection.class );
        when( azureConnectionFactory2.connect( any() ) ).thenReturn( connection2 );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunk1 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        final Set<JobEntry> chunk2 = mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b3 ) );
        for (JobEntry chunk : chunk1) {
            mockDaoDriver.updateBean( chunk.setReadFromAzureTargetId( target.getId() ), ReadFromObservable.READ_FROM_AZURE_TARGET_ID );
        }
        for (JobEntry chunk : chunk2) {
            mockDaoDriver.updateBean( chunk.setReadFromAzureTargetId( target.getId() ), ReadFromObservable.READ_FROM_AZURE_TARGET_ID );
        }

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );

        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnectionFactory1)
                .buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        chunk1.stream().toList() ));

        final ReadChunkFromAzureTargetTask task2 = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnectionFactory2)
                .buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.AZURE,
                        chunk2.stream().toList() ));

        when( connection1.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );

        final boolean[] task2Submitted = { false };
        when( connection1.readBlobFromCloud( any(), any(), any(), any() ) ).thenAnswer( (Answer< List< Future< ? > > >) invocation -> {
            if ( !task2Submitted[0] )
            {
                task2Submitted[0] = true;
                SystemWorkPool.getInstance().submit( task2 );
                Thread.sleep( 100 );
                assertEquals(BlobStoreTaskState.READY, task2.getState(), "Task2 should be blocked while task1 is reading blob b1");
            }
            throw new RuntimeException( "Simulated task1 failure" );
        });

        when( connection2.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );

        final Future< ? > future1 = mock( Future.class );
        final Future< ? > future2 = mock( Future.class );

        when( connection2.readBlobFromCloud( any(), any(), any(), any() ) ).thenAnswer( (Answer< List< Future< ? > > >) invocation -> {
            cacheManager.blobLoadedToCache( b1.getId() );
            cacheManager.blobLoadedToCache( b3.getId() );
            return List.of(future1, future2);
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

        final Object actual1 = task2.getState();
        assertEquals((Object) BlobStoreTaskState.COMPLETED, actual1, "Task2 should succeed after task1 releases locks");
        for (JobEntry chunk : chunk2) {
            final Object actual = mockDaoDriver.attain( chunk ).getBlobStoreState();
            assertEquals((Object) JobChunkBlobStoreState.COMPLETED, actual, "Chunk2 should be completed");
        }
        assertTrue(cacheManager.isOnDisk( b1.getId() ), "Blob b1 should be cached");
        assertTrue(cacheManager.isOnDisk( b3.getId() ), "Blob b3 should be cached");
    }


    @Test
    public void testRunSuspectBlobsAzure() throws Exception {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );

        final AzureConnectionFactory azureConnectionFactory = mock( AzureConnectionFactory.class );
        final AzureConnection connection = mock( AzureConnection.class );

        when( azureConnectionFactory.connect( any() ) ).thenReturn( connection );

        final Future< ? > failedFuture1 = mock( Future.class );
        final Future< ? > failedFuture2 = mock( Future.class );


        when( connection.readBlobFromCloud( any(), any(), any(), any() ) )
                .thenReturn( List.of( failedFuture1 ) )
                .thenReturn( List.of( failedFuture2 ) );

        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.GET, CollectionFactory.toSet( b1, b2 ) );
        for (JobEntry chunk : chunks) {
            mockDaoDriver.updateBean(
                    chunk.setReadFromAzureTargetId(target.getId()),
                    ReadFromObservable.READ_FROM_AZURE_TARGET_ID);
        }
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        final ReadChunkFromAzureTargetTask task = new TargetTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .withAzureConnectionFactory(azureConnectionFactory)
                .buildReadChunkFromAzureTargetTask(new ReadDirective(
                        BlobStoreTaskPriority.NORMAL,
                        target.getId(),
                        PersistenceType.S3,
                        new ArrayList(chunks)
                ));

        final BlobAzureTarget blobAzureTarget = BeanFactory.newBean( BlobAzureTarget.class );
        blobAzureTarget.setBlobId( b1.getId() );
        blobAzureTarget.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobAzureTarget.class ).create( blobAzureTarget );
        mockDaoDriver.makeSuspect( blobAzureTarget );

        final BlobAzureTarget blobAzureTarget2 = BeanFactory.newBean( BlobAzureTarget.class );
        blobAzureTarget2.setBlobId( b2.getId() );
        blobAzureTarget2.setTargetId( target.getId() );
        dbSupport.getServiceManager().getCreator( BlobAzureTarget.class ).create( blobAzureTarget2 );

        when( connection.getExistingBucketInformation( any() ) ).thenReturn(
                BeanFactory.newBean( PublicCloudBucketInformation.class )
                        .setLocalBucketName( bucket.getName() )
                        .setOwnerId( mockDaoDriver.attainOneAndOnly(
                                DataPathBackend.class ).getInstanceId() )
                        .setVersion( 1 ) );

        task.prepareForExecutionIfPossible();


        TestUtil.invokeAndWaitUnchecked( task );


        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completed.");
        assertEquals(JobChunkBlobStoreState.PENDING, mockDaoDriver.getJobEntryFor( b1.getId() ).getBlobStoreState(), "Shoulda reported pending.");
        assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.getJobEntryFor( b2.getId() ).getBlobStoreState(), "Shoulda reported pending.");
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