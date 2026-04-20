/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.*;

import com.google.common.collect.Lists;
import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsToVerify;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class VerifyChunkOnTapeTask_Test
{
    @Test
    public void testPrepareForStartWhenJobChunkNoLongerExistsResultsInException()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        chunk.setReadFromTapeId( tape.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        
        final VerifyChunkOnTapeTask task = createTask(Set.of(chunk), dbSupport.getServiceManager());
        
        dbSupport.getDataManager().deleteBean( JobEntry.class, chunk.getId() );
        
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class,
                () -> task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() ) );
    }
    
    
    @Test
    public void testRunWhenJobChunkNoLongerExistsResultsInTaskCompletion()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        chunk.setReadFromTapeId( tape.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        
        final VerifyChunkOnTapeTask task = createTask(Set.of(chunk), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        dbSupport.getDataManager().deleteBean( JobEntry.class, chunk.getId() );
        dbSupport.getDataManager().deleteBean( Job.class, job.getId() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda returned completed.");
    }
    
    
    @Test
    public void testRunWhenJobChunkNoLongerContainsAnyJobEntriesResultsInTaskCompletion()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        chunk.setReadFromTapeId( tape.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final VerifyChunkOnTapeTask task = createTask(Set.of(chunk), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        dbSupport.getDataManager().deleteBeans( JobEntry.class, Require.nothing() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda returned completed.");
    }
    
    
    @Test
    public void testRunWhenOnlyTapeThatHasDataOnItIsNonNormalStateDoesNotWork()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        for (final JobEntry chunk : chunks) {
            chunk.setReadFromTapeId( tape.getId() );
        }

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");
                            cacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
                            cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                            cacheManager.blobLoadedToCache( blob1.getId() );
                            cacheManager.blobLoadedToCache( blob2.getId() );
                            return null;
                        },
                        null ) ) );

        final VerifyChunkOnTapeTask task = createTask(chunks, dbSupport.getServiceManager());
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class, () -> task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setVerifyAvailableException(
                        new RuntimeException( "I'm not in a normal state." ) ) ) );

        for ( final JobEntry chunk : chunks ) {
            assertEquals(JobChunkBlobStoreState.PENDING, dbSupport.getServiceManager().getRetriever( JobEntry.class ).attain( chunk.getId() )
                        .getBlobStoreState(), "Should notta modified job chunk blob store state.");
        }
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWithoutFailuresWorksWhenObjectDoesNotHaveCreationDate()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        for (final JobEntry chunk : chunks) {
            chunk.setReadFromTapeId( tape.getId() );
        }

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final S3ObjectsToVerify [] requestHolder = new S3ObjectsToVerify[ 1 ];
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            final S3ObjectsToVerify request = (S3ObjectsToVerify)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            return null;
                        },
                        null ) ) );

        final VerifyChunkOnTapeTask task = createTask(chunks, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( td1 )
                                                                                         .setTapePartitionId(
                                                                                                 partitionId ) );

        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda permitted reading of the blobs.");

        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        final Object expected = blob1.getLength() + blob2.getLength();
        assertEquals(expected, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(0,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda whacked all job entries since they'll not be read by the client.");

        final S3ObjectOnMedia oom1;
        final S3ObjectOnMedia oom2;
        if ( "o1".equals( requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ].getObjectName() ) )
        {
            oom1 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ];
            oom2 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 1 ];
        }
        else
        {
            oom1 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 1 ];
            oom2 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ];
        }
        assertEquals("o1", oom1.getObjectName(), "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(1,  oom1.getBlobs().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals("o2", oom2.getObjectName(), "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(1,  oom2.getBlobs().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(4,  oom1.getMetadata().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(2,  oom2.getMetadata().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");

    }
    
    
    @Test
    public void testRunWithoutFailuresWorksWhenObjectHasCreationDate()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        for (final JobEntry chunk : chunks) {
            chunk.setReadFromTapeId( tape.getId() );
        }
        mockDaoDriver.simulateObjectUploadCompletion( o1.getId() );
        mockDaoDriver.simulateObjectUploadCompletion( o2.getId() );

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final S3ObjectsToVerify [] requestHolder = new S3ObjectsToVerify[ 1 ];
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            final S3ObjectsToVerify request = (S3ObjectsToVerify)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            return null;
                        },
                        null ) ) );

        final VerifyChunkOnTapeTask task = createTask(chunks, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( td1 )
                                                                                         .setTapePartitionId( partitionId ) );

        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda permitted reading of the blobs.");

        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        final Object expected2 = blob1.getLength() + blob2.getLength();
        assertEquals(expected2, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(0,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda whacked all job entries since they'll not be read by the client.");

        final S3ObjectOnMedia oom1;
        final S3ObjectOnMedia oom2;
        if ( "o1".equals( requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ].getObjectName() ) )
        {
            oom1 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ];
            oom2 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 1 ];
        }
        else
        {
            oom1 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 1 ];
            oom2 = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ];
        }
        assertEquals("o1", oom1.getObjectName(), "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(1,  oom1.getBlobs().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals("o2", oom2.getObjectName(), "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(1,  oom2.getBlobs().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(4,  oom1.getMetadata().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(2,  oom2.getMetadata().length, "Request sent down shoulda been as expected, with metadata sent.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");

        final Map< String, String > oom2Metadata = new HashMap<>();
        for ( final S3ObjectMetadataKeyValue kv : oom2.getMetadata() )
        {
            oom2Metadata.put( kv.getKey(), kv.getValue() );
        }
        final Object expected1 = oom2Metadata.get( KeyValueObservable.TOTAL_BLOB_COUNT );
        assertEquals(expected1, "1", "Shoulda sent down blob count.");
        final Object expected = oom2Metadata.get( KeyValueObservable.CREATION_DATE );
        assertEquals(expected, String.valueOf( mockDaoDriver.attain( S3Object.class, o2 ).getCreationDate().getTime() ), "Shoulda sent down blob count.");
    }
    
    
    @Test
    public void testRunWithFailureRetriesAndEventuallyRecognizesDataLossWhenTwoDrivesUsed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry entry1 = mockDaoDriver.createJobEntry(job.getId(), blob1).setReadFromTapeId(tape.getId());
        final JobEntry entry2 = mockDaoDriver.createJobEntry(job.getId(), blob2).setReadFromTapeId(tape.getId());
        mockDaoDriver.updateBean(entry1, ReadFromObservable.READ_FROM_TAPE_ID);
        mockDaoDriver.updateBean(entry2, ReadFromObservable.READ_FROM_TAPE_ID);
        
        final S3ObjectsToVerify [] requestHolder = new S3ObjectsToVerify[ 1 ];
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            final S3ObjectsToVerify request = (S3ObjectsToVerify)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            
                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final VerifyChunkOnTapeTask task = createTask(Set.of(entry1, entry2), dbSupport.getServiceManager(), tapeFailureManagement);
        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(2,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down both objects to get.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        final Object expected3 = blob2.getLength();
        assertEquals(expected3, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        assertEquals(JobChunkBlobStoreState.PENDING, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(entry1.getId())
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever(JobEntry.class).retrieve(entry2.getId()), "Shoulda deleted entry that finished from the blob store.");
        assertNotNull(mockDaoDriver.attain(entry1).getReadFromTapeId(), "Should notta marked job chunk as needing re-chunking yet.");

        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        final Object expected2 = blob2.getLength();
        assertEquals(expected2, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertNotEquals(JobChunkBlobStoreState.COMPLETED, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(entry1.getId())
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        assertNotNull(mockDaoDriver.attain(entry1).getReadFromTapeId(), "Should notta marked job chunk as needing re-chunking yet.");

        assertFalse(task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) ), "Shoulda decided that drive1 has been exhausted for read attempts.");
        assertFalse(task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) ), "Shoulda been willing to use other drive.");

        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        final Object expected1 = blob2.getLength();
        assertEquals(expected1, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(JobChunkBlobStoreState.PENDING, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(entry1.getId())
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        assertNotNull(mockDaoDriver.attain(entry1).getReadFromTapeId(), "Should notta marked job chunk as needing re-chunking yet.");

        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        final Object expected = blob2.getLength();
        assertEquals(expected, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        assertEquals(JobChunkBlobStoreState.PENDING, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(entry1.getId())
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store.");
        assertNull(mockDaoDriver.attain(entry1).getReadFromTapeId(), "Should be marked job for re-chunking.");
        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td1 ).contains(tape.getId()), "Should have cleared strikes against drive1.");
        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td2 ).contains(tape.getId()), "Should be no strikes against drive2.");
    }
    
    
    @Test
    public void testRunWithFailureRetriesAndEventuallyRecognizesDataLossWhenThreeDrivesUsed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() ).getId();
        final UUID td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" ).getId();
        final UUID td3 = mockDaoDriver.createTapeDrive( partitionId, "tdsn3" ).getId();
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );
        final JobEntry entry1 = mockDaoDriver.createJobEntry(job.getId(), blob1)
                .setReadFromTapeId(tape.getId())
                .setBlobStoreState(JobChunkBlobStoreState.IN_PROGRESS);
        final JobEntry entry2 = mockDaoDriver.createJobEntry(job.getId(), blob2)
                .setReadFromTapeId(tape.getId())
                .setBlobStoreState(JobChunkBlobStoreState.IN_PROGRESS);
        mockDaoDriver.updateBean(entry1, ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE);
        mockDaoDriver.updateBean(entry2, ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE);
        
        final S3ObjectsToVerify [] requestHolder = new S3ObjectsToVerify[ 1 ];
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            final S3ObjectsToVerify request = (S3ObjectsToVerify)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            
                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );

        final VerifyChunkOnTapeTask task = createTask(Set.of(entry1, entry2), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        
        Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(2,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down both objects to get.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        final Object expected2 = blob2.getLength();
        assertEquals(expected2, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever( JobEntry.class ).attain( entry1.getId() )
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever(JobEntry.class).retrieve(entry2.getId()), "Shoulda deleted entry that finished from the blob store.");
        assertNotNull(mockDaoDriver.attain(entry1).getReadFromTapeId(), "Should notta marked job chunk as needing re-chunking yet.");

        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        final Object expected1 = blob2.getLength();
        assertEquals(expected1, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(entry1.getId())
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        assertNotNull(mockDaoDriver.attain(entry1).getReadFromTapeId(), "Should notta marked job chunk as needing re-chunking yet.");

        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        final Object expected = blob2.getLength();
        assertEquals(expected, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(entry1.getId())
                        .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store.");
    }
    
    
    @Test
    public void testRunWithInternalExceptionWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        for (final JobEntry chunk : chunks) {
            chunk.setReadFromTapeId( tape.getId() );
        }
        
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            throw new RuntimeException( "Oops." );
                        },
                        null ) ) );

        final VerifyChunkOnTapeTask task = createTask(chunks, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.assertThrows(
                null,
                RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );
        
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta modified any state, due to exception.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        for ( final JobEntry chunk : chunks ) {
            assertEquals(JobChunkBlobStoreState.PENDING, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta modified job chunk blob store state.");
        }
    }
    
    
    @Test
    public void testRunWithRpcExceptionWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.VERIFY );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        for (final JobEntry chunk : chunks) {
            chunk.setReadFromTapeId( tape.getId() );
        }
        
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "verifyData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );

        final VerifyChunkOnTapeTask task = createTask(chunks, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.assertThrows(
                null,
                RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );
        
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta modified any state, due to exception.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.VERIFY_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        for ( final JobEntry chunk : chunks ) {
            assertEquals(JobChunkBlobStoreState.PENDING, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta modified job chunk blob store state.");
        }
    }

    private VerifyChunkOnTapeTask createTask(Set<JobEntry> chunks, BeansServiceManager serviceManager) {
        return createTask(chunks, serviceManager, new TapeFailureManagement(serviceManager));
    }

    private VerifyChunkOnTapeTask createTask(Set<JobEntry> chunks, BeansServiceManager serviceManager, TapeFailureManagement tapeFailureManagement) {
        return new VerifyChunkOnTapeTask( BlobStoreTaskPriority.values()[ 0 ],
                Lists.newArrayList(chunks),
                new MockDiskManager(serviceManager),
                new JobProgressManagerImpl( serviceManager, BufferProgressUpdates.NO ),
                tapeFailureManagement, serviceManager);
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
