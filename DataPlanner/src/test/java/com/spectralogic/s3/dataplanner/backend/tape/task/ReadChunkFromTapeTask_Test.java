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
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public final class ReadChunkFromTapeTask_Test
{

    @Test
    public void testPrepareForStartWhenJobChunkNoLongerExistsResultsInException()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.updateBean(
                chunk.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        
        dbSupport.getDataManager().deleteBean( JobEntry.class, chunk.getId() );
        dbSupport.getDataManager().deleteBean( Job.class, job.getId() );
        
        final ReadChunkFromTapeTask task =
                new ReadChunkFromTapeTask( BlobStoreTaskPriority.values()[ 0 ],
                        CollectionFactory.toList( chunk ),
                        new MockDiskManager(dbSupport.getServiceManager()),
                        new JobProgressManagerImpl(dbSupport.getServiceManager()),
                        new TapeFailureManagement(dbSupport.getServiceManager()),
                        dbSupport.getServiceManager());
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class,
                () -> task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() ) );
    }
    
    
    @Test
    public void testRunWhenJobChunkNoLongerExistsResultsInTaskCompletion()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.updateBean(
                chunk.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final ReadChunkFromTapeTask task = createTask( chunk, new MockDiskManager(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        dbSupport.getDataManager().deleteBean( JobEntry.class, chunk.getId() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda returned completed.");
    }
    
    
    @Test
    public void testPrepareForStartFailsToSelectTapeIfCannotAllocateCacheSpace()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.updateBean(
                chunk.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        
        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.setOutOfSpace( true );
        final ReadChunkFromTapeTask task = createTask( chunk, cacheManager, dbSupport.getServiceManager());

        TestUtil.assertThrows( null, RuntimeException.class,
                () -> task.prepareForExecutionIfPossible( new MockTapeDriveResource(), new MockTapeAvailability() ) );

        assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever( JobEntry.class ).attain( chunk.getId() )
                        .getBlobStoreState(), "Should notta modified job chunk blob store state.");
    }
    
    
    @Test
    public void testRunWhenOnlyTapeThatHasDataOnItIsNonNormalStateDoesNotWork()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
            mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        }

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");
                            mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
                            mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                            return null;
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());;
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class, () -> task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setVerifyAvailableException(
                        new RuntimeException( "Tape is in non-normal state." ) ) ) );

        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta modified job chunk blob store state.");
        }
        mockCacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWithoutFailuresWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
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
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
        mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
            mockDaoDriver.updateBean( entry, ReadFromObservable.READ_FROM_TAPE_ID );
        }

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final ReadChunkFromTapeTask task2 = createTask( chunks, cacheManager, dbSupport.getServiceManager());
        task2.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( td1 )
                                                                                          .setTapePartitionId(
                                                                                                  partitionId ) );
    
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");

                            TestUtil.invokeAndWaitUnchecked( task2 );

                            assertEquals(BlobStoreTaskState.READY, task2.getState(), "Should notta permitted concurrent reading of the same blobs.");
                            cacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
                            cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                            return null;
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( td1 )
                                                                                         .setTapePartitionId(
                                                                                                 partitionId ) );

        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda permitted reading of the blobs.");

        assertNotNull(cacheManager.getDiskFileFor( blob1.getId() ), "Shoulda returned cache file.");
        assertNotNull(cacheManager.getDiskFileFor( blob2.getId() ), "Shoulda returned cache file.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        final Object expected = blob1.getLength() + blob2.getLength();
        assertEquals(expected, updatedJob.getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda retained all job entries since they've not been read by the client yet.");

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
        assertEquals("o1", oom1.getObjectName(), "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(1,  oom1.getBlobs().length, "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals("o2", oom2.getObjectName(), "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(1,  oom2.getBlobs().length, "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(null, oom1.getMetadata(), "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(null, oom2.getMetadata(), "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        TestUtil.assertThrows( 
                "Should notta been a need to read into cache anymore.",
                BlobStoreTaskNoLongerValidException.class,
                () -> task2.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() ) );

        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Shoulda marked chunk as having been completed by the blob store.");
            assertNotNull(mockDaoDriver.attain(chunk).getReadFromTapeId(), "Should notta whacked read from target.");
        }
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWhereOneEntryIsAlreadyInCacheWithoutFailuresWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
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
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
        mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        }

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");
                            cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                            return null;
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( td1 )
                                                                                         .setTapePartitionId(
                                                                                                 partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(cacheManager.getDiskFileFor( blob1.getId() ), "Shoulda returned cache file.");
        assertNotNull(cacheManager.getDiskFileFor( blob2.getId() ), "Shoulda returned cache file.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        final Object expected = blob1.getLength() + blob2.getLength();
        assertEquals(expected, updatedJob.getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda retained all job entries since they've not been read by the client yet.");

        final S3ObjectOnMedia oom = requestHolder[ 0 ].getBuckets()[ 0 ].getObjects()[ 0 ];
        assertEquals("o2", oom.getObjectName(), "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(1,  oom.getBlobs().length, "Request sent down shoulda been as expected, with no metadata sent.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");

        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Shoulda marked chunk as having been completed by the blob store.");
        }
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWhereAllEntriesAreAlreadyInCacheWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
        mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        }

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");
                            return null;
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(cacheManager.getDiskFileFor( blob1.getId() ), "Shoulda returned cache file.");
        assertNotNull(cacheManager.getDiskFileFor( blob2.getId() ), "Shoulda returned cache file.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        final Object expected = blob1.getLength() + blob2.getLength();
        assertEquals(expected, updatedJob.getCachedSizeInBytes(), "Shoulda updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Shoulda retained all job entries since they've not been read by the client yet.");

        assertEquals(null, requestHolder[ 0 ], "Should notta sent down request.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");

        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.COMPLETED, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Shoulda marked chunk as having been completed by the blob store.");
        }
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWithFailureRetriesAndEventuallyRecognizesDataLossWhenTwoDrivesUsed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        final TapeDrive td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() );
        final TapeDrive td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry chunk : chunks) {
        mockDaoDriver.updateBean( 
                chunk.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                    ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE);
        }
        
        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");
                            cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                            
                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );

        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(serviceManager);
        final ReadChunkFromTapeTask task = new ReadChunkFromTapeTask(BlobStoreTaskPriority.values()[0],
                Lists.newArrayList(chunks),
                cacheManager,
                new JobProgressManagerImpl( serviceManager, BufferProgressUpdates.NO ),
                tapeFailureManagement,
                serviceManager);
        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1.getId() ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        TestUtil.assertThrows(
                "Blob 1 failed, so should notta been in cache.",
                DataPlannerException.class, () -> cacheManager.getDiskFileFor( blob1.getId() ) );
        assertNotNull(cacheManager.getDiskFileFor( blob2.getId() ), "Shoulda returned cache file.");
        Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(2,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down both objects to get.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        }

        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1.getId() ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        }
        assertTrue(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td1.getId() ).contains(tape.getId()), "Shoulda decided that drive1 has been exhausted for read attempts.");
        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td2.getId() ).contains(tape.getId()), "Shoulda been willing to use drive2.");
        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2.getId() ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1, requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");

            assertNotNull(mockDaoDriver.attain(chunk).getReadFromTapeId(), "Should notta marked chunk as needing to be re-chunked yet.");
        }

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2.getId() ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(blob2.getLength(), updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        final JobEntry chunk1 = mockDaoDriver.getJobEntryFor(blob1.getId());
        final JobEntry chunk2 = mockDaoDriver.getJobEntryFor(blob2.getId());
        assertEquals(null, chunk1.getReadFromTapeId(), "Shoulda marked chunk as needing to be re-chunked.");
        assertEquals(JobChunkBlobStoreState.COMPLETED, chunk2.getBlobStoreState(), "Shoulda marked chunk as having not been completed by the blob store.");
        assertNotNull(chunk2.getReadFromTapeId(), "Shoulda marked chunk as needing to be re-chunked.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as having completed.");

        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td1.getId() ).contains(tape.getId()), "Should have cleared strikes against drive1.");
        assertFalse(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive( td2.getId() ).contains(tape.getId()), "Should be no strikes against drive2.");
        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWithFailureRetriesAndEventuallyRecognizesDataLossWhenThreeDrivesUsed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
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
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
        mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        }
        
        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];
                            requestHolder[ 0 ] = request;
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob1.getId() ), "Shoulda already allocated cache space.");
                            assertTrue(cacheManager.isCacheSpaceAllocated( blob2.getId() ), "Shoulda already allocated cache space.");
                            cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                            
                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        TestUtil.assertThrows(
                "Blob 1 failed, so should notta been in cache.",
                DataPlannerException.class, () -> cacheManager.getDiskFileFor( blob1.getId() ) );
        assertNotNull(cacheManager.getDiskFileFor( blob2.getId() ), "Shoulda returned cache file.");
        Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(2,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down both objects to get.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        }
        
        task.prepareForExecutionIfPossible( 
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1, requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job yet.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure yet.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store yet.");
        }
        
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td3 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );
        
        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );

        assertEquals(1,  requestHolder[0].getBuckets()[0].getObjects().length, "Shoulda sent down just the object we failed to get the first time.");
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(blob2.getLength(),  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta deleted the job entry that was lost due to the blob read failure.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.BLOB_READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");

        final JobEntry chunk1 = mockDaoDriver.getJobEntryFor(blob1.getId());
        final JobEntry chunk2 = mockDaoDriver.getJobEntryFor(blob2.getId());
        assertEquals(JobChunkBlobStoreState.IN_PROGRESS, chunk1.getBlobStoreState(), "Shoulda marked chunk as having not been completed by the blob store.");
        assertEquals(null, chunk1.getReadFromTapeId(), "Shoulda marked chunk as needing to be re-chunked.");
        assertEquals(JobChunkBlobStoreState.COMPLETED, chunk2.getBlobStoreState(), "Shoulda marked chunk as having not been completed by the blob store.");
        assertNotNull(chunk2.getReadFromTapeId(), "Shoulda marked chunk as needing to be re-chunked.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as having completed.");

        cacheFilesystemDriver.shutdown();
    }
    
    
    @Test
    public void testRunWithInternalExceptionWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
        mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        }
        
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            throw new RuntimeException( "Oops." );
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());
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

        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store.");
        }
    }
    
    
    @Test
    public void testRunWithRpcExceptionWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2);
        for (JobEntry entry : chunks ) {
            mockDaoDriver.updateBean(
                entry.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );
        }
        
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );

        final ReadChunkFromTapeTask task = createTask( chunks, cacheManager, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.assertThrows(
                null,
                RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );
        
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta modified any state, due to exception.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta modified any state, due to exception.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.READ_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");

        for (JobEntry chunk : chunks) {
            assertEquals(JobChunkBlobStoreState.IN_PROGRESS, dbSupport.getServiceManager().getRetriever(JobEntry.class).attain(chunk.getId())
                                .getBlobStoreState(), "Should notta marked chunk as having been completed by the blob store.");
        }
    }
    
    
    @Test
    public void testRunForJobWithoutInOrderProcessingGuaranteeAllocatesOnlyTheCurrentChunk()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob3.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        dbSupport.getServiceManager().getService( JobService.class ).update(
                job.setChunkClientProcessingOrderGuarantee( JobChunkClientProcessingOrderGuarantee.NONE ),
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(job.getId(), blob1, blob2, blob3);
        for (JobEntry chunk : chunks) {
            chunk.setReadFromTapeId( tape.getId() );
        }
        
        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final DiskManager cacheManager = InterfaceProxyFactory.getProxy( DiskManager.class, btih );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        
        final ReadChunkFromTapeTask task = createTask(chunks.iterator().next(), cacheManager, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        
        final List< MethodInvokeData > invokeDatas = btih.getMethodInvokeData(
                ReflectUtil.getMethod( DiskManager.class, "allocateChunks" ) );
        assertEquals(1,  invokeDatas.size(), "Shoulda allocated the current chunk.");
        final Object expected = CollectionFactory.toSet( chunks.iterator().next().getId() );
        assertEquals(expected, invokeDatas.get( 0 ).getArgs().get( 0 ), "Shoulda allocated the current chunk.");
    }

    private ReadChunkFromTapeTask createTask(Set<JobEntry> chunks, DiskManager cacheManager, BeansServiceManager serviceManager) {
        return new ReadChunkFromTapeTask(BlobStoreTaskPriority.values()[0],
                Lists.newArrayList(chunks),
                cacheManager,
                new JobProgressManagerImpl( serviceManager, BufferProgressUpdates.NO ),
                new TapeFailureManagement(serviceManager),
                serviceManager);
    }

    @Test
    public void testRunWithFailureCausesRechunkingWhenBlobFailsThreeTimesOnTwoDrives()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );

        final UUID partitionId = mockDaoDriver.createTapePartition( null, "tp1" ).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );

        final TapeDrive td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() );
        final TapeDrive td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" );

        mockDaoDriver.putBlobOnTape( tape.getId(), blob1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.GET );

        final JobEntry chunk1 = mockDaoDriver.createJobEntry( job.getId(), blob1 );
        mockDaoDriver.updateBean(
                chunk1.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );

        final JobEntry chunk2 = mockDaoDriver.createJobEntry( job.getId(), blob2 );
        mockDaoDriver.updateBean(
                chunk2.setBlobStoreState( JobChunkBlobStoreState.IN_PROGRESS ).setReadFromTapeId(tape.getId()),
                ReadFromObservable.READ_FROM_TAPE_ID, JobEntry.BLOB_STORE_STATE );

        
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );

        final int[] failureCount = {0};
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy( 
                TapeDriveResource.class, 
                MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( TapeDriveResource.class, "readData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 0 ];

                            cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );

                            boolean blob1InRequest = false;
                            boolean blob2InRequest = false;
                            for (final S3ObjectOnMedia obj : request.getBuckets()[0].getObjects()) {
                                if ("o1".equals(obj.getObjectName())) {
                                    blob1InRequest = true;
                                }
                                if ("o2".equals(obj.getObjectName())) {
                                    blob2InRequest = true;
                                }
                            }
                            
                            if (blob1InRequest) {
                                cacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
                            }

                            if (blob2InRequest) {
                                failureCount[0]++;
                                if (failureCount[0] <= 4) {
                                    final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                            .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                            .setBlobId( blob2.getId() );
                                    final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                                    retval.setFailures( new BlobIoFailure [] { failure } );
                                    return new RpcResponse<>( retval );
                                } else {
                                    cacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
                                }
                            }
                            
                            return null;
                        },
                        null ) ) );

        final BeansServiceManager serviceManager = dbSupport.getServiceManager();

        final ReadChunkFromTapeTask task = createTask( CollectionFactory.toSet(chunk1, chunk2), cacheManager, dbSupport.getServiceManager());

        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1.getId() ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Task should be ready for retry after first failure.");

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2.getId() ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Task should be ready for retry after second failure.");

        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td1.getId() ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource, new MockTapeAvailability().setDriveId( td2.getId() ).setTapePartitionId( partitionId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(4,  serviceManager.getRetriever(TapeFailure.class).getCount(), "Should have generated 4 tape failures for blob2.");

        final List<SuspectBlobTape> suspectBlobs = serviceManager.getRetriever( SuspectBlobTape.class ).retrieveAll(
                Require.all(
                        Require.beanPropertyEquals( BlobTape.TAPE_ID, tape.getId() ),
                        Require.beanPropertyEquals( BlobTape.BLOB_ID, blob2.getId() )
                )
        ).toList();

        assertEquals(1,  suspectBlobs.size(), "Blob2 should be marked suspect.");

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda marked task as having completed.");
        assertNotNull(mockDaoDriver.attain( chunk1 ).getReadFromTapeId(), "Should notta marked chunk1 as needing to be re-chunked.");
        assertNull(mockDaoDriver.attain(chunk2).getReadFromTapeId(), "Shoulda marked chunk2 as needing to be re-chunked.");

        assertEquals(JobChunkBlobStoreState.COMPLETED, mockDaoDriver.attain( chunk1 ).getBlobStoreState());
        assertEquals(JobChunkBlobStoreState.IN_PROGRESS, mockDaoDriver.attain( chunk2 ).getBlobStoreState());
        cacheFilesystemDriver.shutdown();
    }


    private ReadChunkFromTapeTask createTask(JobEntry chunk, DiskManager cacheManager, BeansServiceManager serviceManager) {
        return new ReadChunkFromTapeTask(BlobStoreTaskPriority.values()[0],
                CollectionFactory.toList(chunk),
                cacheManager,
                new JobProgressManagerImpl( serviceManager, BufferProgressUpdates.NO ),
                new TapeFailureManagement(serviceManager),
                serviceManager);
    }
}
