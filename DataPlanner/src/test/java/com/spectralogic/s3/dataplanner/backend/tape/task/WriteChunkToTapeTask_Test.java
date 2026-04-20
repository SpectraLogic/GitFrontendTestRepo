/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 */

package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeResourceFailureCode;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.client.RpcTimeoutException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public final class WriteChunkToTapeTask_Test 
{
    @Test
    public void testPrepareForStartWhenJobChunkNoLongerExistsResultsInException()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );


        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), new MockDiskManager( dbSupport.getServiceManager() ), mockDaoDriver );
        mockDaoDriver.delete( JobEntry.class, chunks );
        dbSupport.getDataManager().deleteBean( Job.class, job.getId() );

        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class, () -> task.prepareForExecutionIfPossible(
                new MockTapeDriveResource(),
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) ) );
    }


    @Test
    public void testRunWhenJobChunkNoLongerExistsThrowsException()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );


        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), new MockDiskManager(dbSupport.getServiceManager() ), mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                new MockTapeDriveResource(),
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        mockDaoDriver.delete( JobEntry.class, chunks );
        dbSupport.getDataManager().deleteBean( Job.class, job.getId() );

        TestUtil.assertThrows( "shoulda thrown exception since chunk list has changed", RuntimeException.class, () -> {
            TestUtil.invokeAndWaitUnchecked( task );
        });
    }

    @Test
    public void testRunWhenFileNotInCacheThrowsException()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId(), blob );
        final UUID nodeId = mockDaoDriver.attainOneAndOnly(Node.class).getId();
        new MockCacheFilesystemDriver( dbSupport );

        final MockDiskManager cacheManager = new MockDiskManager( dbSupport.getServiceManager() );
        cacheManager.allocateChunk(chunk.getId());
        cacheManager.blobLoadedToCache(blob.getId());
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );
        
        final WriteChunkToTapeTask task = createTask(CollectionFactory.toList(chunk), getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.assertThrows( "shoulda thrown exception since file not in cache", RuntimeException.class, () -> {
            TestUtil.invokeAndWaitUnchecked( task );
        });
    }

    @Test
    public void testPrepareForStartFailsToSelectTapeIfCannotAllocateTape()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO6 );


        final MockDiskManager cacheManager = new MockDiskManager( mockDaoDriver.getServiceManager() );
        cacheManager.setOutOfSpace( true );
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        TestUtil.assertThrows( "shoulda thrown exception when preparing for execution since tape not available", RuntimeException.class, () -> {
            task.prepareForExecutionIfPossible(
                    new MockTapeDriveResource(),
                    new MockTapeAvailability() );
        });
    }


    @Test
    public void testObjectsWithMetadataHaveMetadataSentDownWhenObjectNotYetFullyCreated()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final BasicTestsInvocationHandler btih =
                new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            requestHolder[ 0 ] = request;
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) );
        tapeDriveResource.setInvocationListener(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );

        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
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
        assertEquals("o1", oom1.getObjectName(), "Request sent down shoulda been as expected, with metadata for o1 only.");
        assertEquals("o2", oom2.getObjectName(), "Request sent down shoulda been as expected, with metadata for o1 only.");
        assertEquals(4,  oom1.getMetadata().length, "Request sent down shoulda been as expected, with metadata for o1 only.");
        assertEquals(2,  oom2.getMetadata().length, "Request sent down shoulda been as expected, with metadata for o1 only.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testObjectsWithMetadataHaveMetadataSentDownWhenObjectFullyCreated()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );

        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final Date date = new Date();
        dbSupport.getServiceManager().getService( S3ObjectService.class ).update(
                o1.setCreationDate( date ), S3Object.CREATION_DATE );
        dbSupport.getServiceManager().getService( S3ObjectService.class ).update(
                o2.setCreationDate( date ), S3Object.CREATION_DATE );

        final Map< String, String > objectProperties = new HashMap<>();
        objectProperties.put( "key1", "value1" );
        objectProperties.put( "key2", "value2" );
        mockDaoDriver.createObjectProperties( o1.getId(), objectProperties );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        final S3ObjectsIoRequest [] requestHolder = new S3ObjectsIoRequest[ 1 ];
        final BasicTestsInvocationHandler btih =
                new BasicTestsInvocationHandler( MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            requestHolder[ 0 ] = request;
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) );
        tapeDriveResource.setInvocationListener(
                InterfaceProxyFactory.getProxy( TapeDriveResource.class, btih ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );

        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
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
        assertEquals("o1", oom1.getObjectName(), "Request sent down shoulda been as expected, with metadata for o1 only.");
        assertEquals("o2", oom2.getObjectName(), "Request sent down shoulda been as expected, with metadata for o1 only.");
        assertEquals(4,  oom1.getMetadata().length, "Request sent down shoulda been as expected, with metadata for o1 only.");
        assertEquals(2,  oom2.getMetadata().length, "Request sent down shoulda been as expected, with metadata for o1 only.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithVerifyAfterWriteAndNoVerifyFailureWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean( job.setVerifyAfterWrite( true ), Job.VERIFY_AFTER_WRITE );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final Bucket bucket =
                dbSupport.getServiceManager().getRetriever( Bucket.class ).attain( Require.nothing() );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final LtfsFileNamingMode ltfsCompatibilityLevel =
                                    (LtfsFileNamingMode)args[ 0 ];
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(LtfsFileNamingMode.OBJECT_ID, ltfsCompatibilityLevel, "Shoulda sent down naming mode configured by storage domain.");
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( false ),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        assertEquals(0,  ejectBtih.getTotalCallCount(), "Should notta ejected tape.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithVerifyAfterWriteAndVerifyFailureHandledAsFailure()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean( job.setVerifyAfterWrite( true ), Job.VERIFY_AFTER_WRITE );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final Bucket bucket =
                dbSupport.getServiceManager().getRetriever( Bucket.class ).attain( Require.nothing() );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } ) );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final LtfsFileNamingMode ltfsCompatibilityLevel =
                                    (LtfsFileNamingMode)args[ 0 ];
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(LtfsFileNamingMode.OBJECT_ID, ltfsCompatibilityLevel, "Shoulda sent down naming mode configured by storage domain.");
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( false ),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertTrue(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Should notta updated last checkpoint for tape.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed all job entries since they've been written to tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithoutFailuresWorksWhenStorageDomainNotConfiguredForAutoEject()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final Bucket bucket =
                dbSupport.getServiceManager().getRetriever( Bucket.class ).attain( Require.nothing() );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } ) );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final LtfsFileNamingMode ltfsCompatibilityLevel =
                                    (LtfsFileNamingMode)args[ 0 ];
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(LtfsFileNamingMode.OBJECT_ID, ltfsCompatibilityLevel, "Shoulda sent down naming mode configured by storage domain.");
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( false ),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        assertEquals(0,  ejectBtih.getTotalCallCount(), "Should notta ejected tape.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithoutFailuresWorksWhenStorageDomainConfiguredForAutoEjectButTapeHasLotsOfSpace()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( true ),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih ), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        assertEquals(0,  ejectBtih.getTotalCallCount(), "Should notta ejected tape.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithoutFailuresWorksWhenStorageDomainConfiguredForAutoEjectAndTapeMustBeEjected()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final Bucket bucket =
                dbSupport.getServiceManager().getRetriever( Bucket.class ).attain( Require.nothing() );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( true ).setAutoEjectMediaFullThreshold(2000L),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL, StorageDomain.AUTO_EJECT_MEDIA_FULL_THRESHOLD );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih ), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        assertEquals(1, ejectBtih.getTotalCallCount(), "Shoulda ejected tape.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithoutFailuresWorksWhenStorageDomainConfiguredForAutoEjectButTapeHasLotsOfSpaceTHRSH()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final Bucket bucket =
                dbSupport.getServiceManager().getRetriever( Bucket.class ).attain( Require.nothing() );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( true ),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL );
        mockDaoDriver.updateBean(
                sd.setAutoEjectMediaFullThreshold( 1L ),
                StorageDomain.AUTO_EJECT_MEDIA_FULL_THRESHOLD );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih ), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        assertEquals(0,  ejectBtih.getTotalCallCount(), "Should notta ejected tape.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithoutFailuresWorksWhenStorageDomainConfiguredForAutoEjectAndTapeMustBeEjectedTHRSH()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toList( blob1, blob2 ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            final Object expected3 = o1.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected2 = o2.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected1 = blob1.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob2.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final StorageDomain sd =
                dbSupport.getServiceManager().getRetriever( StorageDomain.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                sd.setAutoEjectUponMediaFull( true ),
                StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL );
        mockDaoDriver.updateBean(
                sd.setAutoEjectMediaFullThreshold( Long.MAX_VALUE ),
                StorageDomain.AUTO_EJECT_MEDIA_FULL_THRESHOLD );
        final BasicTestsInvocationHandler ejectBtih = new BasicTestsInvocationHandler( null );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector( ejectBtih ), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        assertEquals(1,  ejectBtih.getTotalCallCount(), "Shoulda ejected tape.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunForZeroLengthObjectsWithoutFailuresWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", -1 );
        final Blob blob3 = mockDaoDriver.createBlobs( o3.getId(), 1, 0 ).get( 0 );
        final S3Object f4 = mockDaoDriver.createObject( null, "f4/", -1 );
        final Blob blob4 = mockDaoDriver.createBlobs( f4.getId(), 1, 0 ).get( 0 );
        final S3Object f5 = mockDaoDriver.createObject( null, "f5/", -1 );
        final Blob blob5 = mockDaoDriver.createBlobs( f5.getId(), 1, 0 ).get( 0 );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(
                CollectionFactory.toList( blob1, blob2, blob3, blob4, blob5 ) );
        mockDaoDriver.createObjectProperties( o2.getId(), CollectionFactory.toMap( "o2k", "o2v" ) );
        mockDaoDriver.createObjectProperties( f4.getId(), CollectionFactory.toMap( "f4k", "f4v" ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(5,  request.getBuckets()[0].getObjects().length, "Shoulda sent all 5 blobs to write.");
                            final Object expected9 = o1.getId();
                            assertEquals(expected9, request.getBuckets()[ 0 ].getObjects()[ 0 ].getId(), "Shoulda sent object id.");
                            final Object expected8 = o2.getId();
                            assertEquals(expected8, request.getBuckets()[ 0 ].getObjects()[ 1 ].getId(), "Shoulda sent object id.");
                            final Object expected7 = o3.getId();
                            assertEquals(expected7, request.getBuckets()[ 0 ].getObjects()[ 2 ].getId(), "Shoulda sent object id.");
                            final Object expected6 = f4.getId();
                            assertEquals(expected6, request.getBuckets()[ 0 ].getObjects()[ 3 ].getId(), "Shoulda sent object id.");
                            final Object expected5 = f5.getId();
                            assertEquals(expected5, request.getBuckets()[ 0 ].getObjects()[ 4 ].getId(), "Shoulda sent object id.");
                            final Object expected4 = blob1.getId();
                            assertEquals(expected4, request.getBuckets()[ 0 ].getObjects()[ 0 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected3 = blob2.getId();
                            assertEquals(expected3, request.getBuckets()[ 0 ].getObjects()[ 1 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected2 = blob3.getId();
                            assertEquals(expected2, request.getBuckets()[ 0 ].getObjects()[ 2 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected1 = blob4.getId();
                            assertEquals(expected1, request.getBuckets()[ 0 ].getObjects()[ 3 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            final Object expected = blob5.getId();
                            assertEquals(expected, request.getBuckets()[ 0 ].getObjects()[ 4 ].getBlobs()[ 0 ].getId(), "Shoulda sent blob id.");
                            assertEquals(2,  request.getBuckets()[0].getObjects()[0].getMetadata().length, "Shoulda sent metadata for o2 and f4.");
                            assertEquals(3,  request.getBuckets()[0].getObjects()[1].getMetadata().length, "Shoulda sent metadata for o2 and f4.");
                            assertEquals(2,  request.getBuckets()[0].getObjects()[2].getMetadata().length, "Shoulda sent metadata for o2 and f4.");
                            assertEquals(3,  request.getBuckets()[0].getObjects()[3].getMetadata().length, "Shoulda sent metadata for o2 and f4.");
                            assertEquals(2,  request.getBuckets()[0].getObjects()[4].getMetadata().length, "Shoulda sent metadata for o2 and f4.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        cacheManager.allocateChunksForBlob( blob3.getId() );
        cacheManager.allocateChunksForBlob( blob4.getId() );
        cacheManager.allocateChunksForBlob( blob5.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheFilesystemDriver.writeCacheFile( blob3.getId(), 0 );
        cacheFilesystemDriver.writeCacheFile( blob4.getId(), 0 );
        cacheFilesystemDriver.writeCacheFile( blob5.getId(), 0 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        cacheManager.blobLoadedToCache( blob3.getId() );
        cacheManager.blobLoadedToCache( blob4.getId() );
        cacheManager.blobLoadedToCache( blob5.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to success.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint().equals( "new" ), "Shoulda updated last checkpoint for tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Should notta whacked zero-length blobs for zero-length objects.");
        assertTrue(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Shoulda marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWhereJobChunkContainsNoJobEntriesResultsInTaskCompletedEarly()
    {
        final DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(
                    CollectionFactory.toSet( DaoDomainsSeed.class ),
                    CollectionFactory.toSet( DaoServicesSeed.class ) );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final AtomicBoolean resourceCalled = new AtomicBoolean();
        new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );

        TestUtil.assertThrows( "Should not have created task with no destinatinos", IllegalArgumentException.class, () -> {
            final WriteChunkToTapeTask task = createTask(Set.of(), getTapeEjector(), cacheManager, mockDaoDriver );
        });

        assertFalse(resourceCalled.get(), "Should notta called tape drive resource.");
    }


    @Test
    public void testRunWhereTapeIsNotInNormalStateNotAllowed()
    {
        final DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(
                    CollectionFactory.toSet( DaoDomainsSeed.class ),
                    CollectionFactory.toSet( DaoServicesSeed.class ) );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertNotNull(dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                                                                tape.getId() ).getSerialNumber(), "Shoulda set the tape's serial number before this task completes.");
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );
                            return null;
                        },
                        null ) ) );


        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        TestUtil.assertThrows( null, IllegalStateException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        assertFalse(resourceCalled.get(), "Should notta called tape drive resource.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
    }


    @Test
    public void testBlobsSentToTapeDriveResourceAndUpdatedInDatabaseInOrderByJobEntryOrderIndex()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 30 );
        final Blob blob3 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o4 = mockDaoDriver.createObject( null, "o4", -1 );
        final List< Blob > blobsForObject4 =
                new ArrayList<>( mockDaoDriver.createBlobs( o4.getId(), 3, 100 ) );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );

        final Set< Blob > blobs = CollectionFactory.toSet( blob1, blob2, blob3 );
        blobs.addAll( blobsForObject4 );
        final List<JobEntry> jobEntries =
                new ArrayList<>( BeanUtils.sort( mockDaoDriver.createJobEntries(blobs ) ) );
        assertEquals(6,  blobs.size(), "Shoulda been 6 blobs total.");
        assertEquals(6,  jobEntries.size(), "Shoulda been 6 blobs total.");

        int orderIndex = 0;
        final List< UUID > correctBlobOrder = new ArrayList<>();
        for ( final JobEntry e : jobEntries )
        {
            correctBlobOrder.add( e.getBlobId() );
            final Object expected = ++orderIndex;
            assertEquals(expected, e.getChunkNumber(), "Shoulda been ordered by order index.");
        }

        final List< UUID > blobOrderSentDown = new ArrayList<>();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(4,  request.getBuckets()[0].getObjects().length, "Shoulda sent all objects to write.");
                            for ( final S3ObjectIoRequest o : request.getBuckets()[ 0 ].getObjects() )
                            {
                                for ( final BlobIoRequest b : o.getBlobs() )
                                {
                                    blobOrderSentDown.add( UUID.fromString( Paths.get( b.getFileName() )
                                                                                 .getFileName()
                                                                                 .toString() ) );
                                }
                            }
                            return null;
                        },
                        null ) ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        cacheManager.allocateChunksForBlob( blob3.getId() );
        for ( final Blob b : blobsForObject4 )
        {
            cacheManager.allocateChunksForBlob( b.getId() );
        }

        final WriteChunkToTapeTask task = createTask(jobEntries, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheFilesystemDriver.writeCacheFile( blob3.getId(), 30 );
        for ( final Blob b : blobsForObject4 )
        {
            cacheFilesystemDriver.writeCacheFile( b.getId(), 100 );
        }

        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        cacheManager.blobLoadedToCache( blob3.getId() );
        for ( final Blob b : blobsForObject4 )
        {
            cacheManager.blobLoadedToCache( b.getId() );
        }


        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(6,  blobOrderSentDown.size(), "Shoulda sent down all 6 blobs");

        orderIndex = 0;
        for ( final UUID blobId : blobOrderSentDown )
        {
            assertEquals(correctBlobOrder.get( orderIndex ), blobId, "Shoulda sent down blobs in the order specified by job entry order indexes.");
            final Object expected = ++orderIndex;
            assertEquals(expected, dbSupport.getServiceManager().getRetriever( BlobTape.class ).attain(
                                BlobObservable.BLOB_ID, blobId ).getOrderIndex(), "Shoulda recorded order index in the order it actually went down.");
        }
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithNonRetryableFailureWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );

                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.OUT_OF_SPACE )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(TapeState.BAD, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getState(), "Shoulda marked tape as bad due to OUT_OF_SPACE for the first time.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Shoulda marked tape as bad due to OUT_OF_SPACE for the first time.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint(), "Should notta updated last checkpoint for tape since there was a failure.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed successful job entries since we rolled back.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithRetryableFailureWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );

                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.UNKNOWN )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0, updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getState(), "Should notta marked tape as bad due to failure for the first time.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as bad due to failure for the first time.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint(), "Should notta updated last checkpoint for tape since there was a failure.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed successful job entries since we rolled back.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithInvalidPoolSourceResultsInEventualSuspectBlobPools()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry(job.getId(), blob1 );
        final UUID nodeId = mockDaoDriver.attainOneAndOnly(Node.class).getId();



        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool(mockDaoDriver.createPool(partition2.getId(), null).getId(), blob1.getId());
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke(final Object proxy, final Method method, final Object[] args )
                    {
                        return BeanFactory.newBean( DiskFileInfo.class )
                                .setFilePath( "/pool/some/file.txt" )
                                .setBlobPoolId(blobPool.getId());
                    }
                } );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ), poolBlobStore );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(1,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );

                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );


        final WriteChunkToTapeTask task = createTask(Set.of(chunk), getTapeEjector(), cacheManager, mockDaoDriver );;


        for (int i = 0; i < 3; i++) {
            assertEquals(0,  dbSupport.getServiceManager()
                    .getRetriever(SuspectBlobPool.class)
                    .getCount(), "Should be no suspect blobs yet.");
            assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready due to write error.");
            task.prepareForExecutionIfPossible(
                    tapeDriveResource,
                    new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
            assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
            TestUtil.invokeAndWaitUnchecked( task );
        }

        assertEquals(BlobStoreTaskState.NOT_READY, task.getState(), "Shoulda reported ready due to write error.");
        final Object expected = blobPool.getId();
        assertEquals(expected, mockDaoDriver.attainOneAndOnly(SuspectBlobPool.class).getId(), "Should be exactly one suspect blob matching our blob pool.");

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class )
                        .attain( tape.getId() ).getState(), "Should notta marked tape as bad due to failure for the first time.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                        .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as bad due to failure for the first time.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class )
                        .attain( tape.getId() ).getLastCheckpoint(), "Should notta updated last checkpoint for tape since there was a failure.");
        assertEquals(1,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed successful job entries since we rolled back.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated tape failures.");
        assertTrue(dbSupport.getServiceManager().getRetriever(TapeFailure.class).retrieveAll().toSet().stream()
                        .allMatch(f -> f.getType() == TapeFailureType.WRITE_SOURCE_FAILED),
                "All failures should be WRITE_SOURCE_FAILED, not WRITE_FAILED, since the source (pool) failed.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testPoolReadFailureDuringIomMigrationMarksDataMigrationInError()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        dbSupport.getServiceManager().getUpdater(Job.class).update(
                dbSupport.getServiceManager().getRetriever(Job.class).attain(job.getId())
                        .setIomType(IomType.STANDARD_IOM),
                Job.IOM_TYPE);
        final JobEntry chunk = mockDaoDriver.createJobEntry(job.getId(), blob1 );

        final DataMigration migration = BeanFactory.newBean(DataMigration.class)
                .setPutJobId(job.getId());
        dbSupport.getServiceManager().getCreator(DataMigration.class).create(migration);

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool(mockDaoDriver.createPool(partition2.getId(), null).getId(), blob1.getId());
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke(final Object proxy, final Method method, final Object[] args )
                    {
                        return BeanFactory.newBean( DiskFileInfo.class )
                                .setFilePath( "/pool/some/file.txt" )
                                .setBlobPoolId(blobPool.getId());
                    }
                } );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ), poolBlobStore );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final BlobIoFailure failure = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );

        final WriteChunkToTapeTask task = createTask(Set.of(chunk), getTapeEjector(), cacheManager, mockDaoDriver );

        assertFalse(dbSupport.getServiceManager().getRetriever(DataMigration.class)
                        .attain(migration.getId()).isInError(),
                "Migration should not be in error yet.");

        for (int i = 0; i < 3; i++) {
            task.prepareForExecutionIfPossible(
                    tapeDriveResource,
                    new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
            TestUtil.invokeAndWaitUnchecked( task );
        }

        assertEquals(blobPool.getId(),
                mockDaoDriver.attainOneAndOnly(SuspectBlobPool.class).getId(),
                "Should be exactly one suspect blob matching our blob pool.");
        assertTrue(dbSupport.getServiceManager().getRetriever(DataMigration.class)
                        .attain(migration.getId()).isInError(),
                "Data migration should be marked in error after pool read failures.");
        assertTrue(dbSupport.getServiceManager().getRetriever(TapeFailure.class).retrieveAll().toSet().stream()
                        .allMatch(f -> f.getType() == TapeFailureType.WRITE_SOURCE_FAILED),
                "Tape failures should be WRITE_SOURCE_FAILED, not WRITE_FAILED.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithDuplicateFailureForSameBlobWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final AtomicBoolean resourceCalled = new AtomicBoolean();
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final S3ObjectsIoRequest request = (S3ObjectsIoRequest)args[ 1 ];
                            assertEquals(2,  request.getBuckets()[0].getObjects().length, "Shoulda sent both blobs to write.");
                            resourceCalled.set( true );

                            final BlobIoFailure failure1 = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.OUT_OF_SPACE )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailure failure2 = BeanFactory.newBean( BlobIoFailure.class )
                                    .setFailure( BlobIoFailureType.CHECKSUM_VALUE_MISMATCH )
                                    .setBlobId( blob1.getId() );
                            final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class );
                            retval.setFailures( new BlobIoFailure [] { failure1, failure2 } );
                            return new RpcResponse<>( retval );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertTrue(resourceCalled.get(), "Shoulda called tape drive resource.");
        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Should notta updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to failure for the first time.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint(), "Should notta updated last checkpoint for tape since there was a failure.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed successful job entries since they've been written to tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWhenTapeIsPhysicallyWriteProtectedWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> null,
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint(), "Should notta updated last checkpoint for tape due to exception.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(0, dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertEquals(true, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                .isWriteProtected(), "Shoulda noted tape is write protected.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithInternalExceptionWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RuntimeException( "Ooops." );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getLastCheckpoint(), "Should notta updated last checkpoint for tape due to exception.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithRpcProxyExceptionContinuouslyEventuallyMarksTapeAsBadIfNoDataOnIt()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final TapeDrive td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() );
        final TapeDrive td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );

        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception yet.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.WRITE_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");

        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception yet.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertFalse(task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() )), "Should notta allowed task on this drive, since we've retried exhaustively on it.");

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td2.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception yet.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td2.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception.");
        assertEquals(TapeState.BAD, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getState(), "Shoulda updated tape's state to BAD.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testRunWithRpcProxyExceptionContinuouslyEventuallyMarksTapeAsFullIfDataOnIt()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final TapeDrive td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1", tape.getId() );
        final TapeDrive td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final S3Object existingObject = mockDaoDriver.createObject( null, "existingObject" );
        mockDaoDriver.putBlobOnTape(
                tape.getId(),
                mockDaoDriver.getBlobFor( existingObject.getId() ).getId() );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );

        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception yet.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertEquals(TapeFailureType.WRITE_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                    .retrieveAll().getFirst().getType(), "Shoulda generated a tape failure.");

        mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
        assertNotNull( task.getTapeId(),
                "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception yet.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");

        assertFalse(task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() )), "Should notta allowed task on this drive, since we've retried exhaustively on it.");

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td2.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception yet.");
        assertEquals(2, dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(3, dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");

        mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
        mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td2.getId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");

        TestUtil.invokeAndWaitUnchecked( task );

        updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0, updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0, updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(true, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Shoulda marked tape as full due to exception.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).getState(), "Should notta updated tape's state to BAD.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    /*
    //TODO: test passes but takes 12 minutes to run thanks to having no way in test to dynamically set task suspension.
    //This test should be uncommented following addition to that capabiility.
    @Test
    public void testSingleTaskCannotMarkTooManyTapesFull()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final TapeDrive td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn1");
        final TapeDrive td2 = mockDaoDriver.createTapeDrive( partitionId, "tdsn2" );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );

        mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());
        final Set<JobChunk> chunks = mockDaoDriver.createJobEntries( null, CollectionFactory.toSet( blob1, blob2 ) );
        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );




        cacheManager.allocateCacheForBlob( blob1.getId() );
        cacheManager.allocateCacheForBlob( blob2.getId() );
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final WriteChunkToTapeTask task = createTask(chunks, persistenceTargetManager, getTapeEjector(), cacheManager, dbSupport.getServiceManager(), tapeFailureManagement );

        for (int i = 0; i < 4; i++) {
            while (task.getState() == BlobStoreTaskState.NOT_READY) {
                TestUtil.sleep(100);
            }
            final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
            mockDaoDriver.updateBean(tape.setSerialNumber("tape" + i), Tape.SERIAL_NUMBER);
            mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
            tapeDriveResource.setTapeId( tape.getId() );
            tapeDriveResource.setTapeSerialNumber(tape.getSerialNumber());
            if (i == 3) {
                final Throwable ex = TestUtil.assertThrows( "Should have thrown exception for marking too many tapes bad", RuntimeException.class, () -> {
                    task.prepareForExecutionIfPossible(
                            tapeDriveResource,
                            new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
                });
                assertTrueMod("Message " + ex.getMessage() + "Should have mentioned task marking too many tapes bad", ex.getMessage().contains("too many tapes"));
                assertEqualsMod("Task should have been marked completed due to invalidation", task.getState(), BlobStoreTaskState.COMPLETED);
                return;
            } else {
                task.prepareForExecutionIfPossible(
                        tapeDriveResource,
                        new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
            }

            assertNotNullMod(
                    "Shoulda selected tape, indicating that task can be executed at this time.",
                    task.getTapeId() );

            TestUtil.invokeAndWaitUnchecked( task );

            Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
            assertEqualsMod(
                    "Shoulda notta updated original size of job.",
                    0,
                    updatedJob.getOriginalSizeInBytes() );
            assertEqualsMod(
                    "Should notta updated cached size of job.",
                    0,
                    updatedJob.getCachedSizeInBytes() );
            assertEqualsMod(
                    "Shoulda updated completed size of job.",
                    0,
                    updatedJob.getCompletedSizeInBytes() );
            assertEqualsMod(
                    "Should notta marked tape as full due to exception yet.",
                    false,
                    dbSupport.getServiceManager().getRetriever( Tape.class )
                            .attain( tape.getId() ).isFullOfData() );
            assertEqualsMod(
                    "Should notta removed any job entries since an exception occurred.",
                    2,
                    dbSupport.getDataManager().getCount( JobChunk.class, Require.nothing() ) );
            assertEqualsMod(
                    "Shoulda generated a tape failure.",
                    TapeFailureType.WRITE_FAILED,
                    dbSupport.getServiceManager().getRetriever( TapeFailure.class )
                            .retrieveAll().getFirst().getType() );

            mockDaoDriver.updateBean(td1.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            mockDaoDriver.updateBean(td2.setTapeId(null), TapeDrive.TAPE_ID);
            task.prepareForExecutionIfPossible(
                    tapeDriveResource,
                    new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() ) );
            assertNotNullMod(
                    "Shoulda selected tape, indicating that task can be executed at this time.",
                    task.getTapeId() );

            TestUtil.invokeAndWaitUnchecked( task );

            updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
            assertEqualsMod(
                    "Shoulda notta updated original size of job.",
                    0,
                    updatedJob.getOriginalSizeInBytes() );
            assertEqualsMod(
                    "Should notta updated cached size of job.",
                    0,
                    updatedJob.getCachedSizeInBytes() );
            assertEqualsMod(
                    "Shoulda updated completed size of job.",
                    0,
                    updatedJob.getCompletedSizeInBytes() );
            assertEqualsMod(
                    "Should notta marked tape as full due to exception yet.",
                    false,
                    dbSupport.getServiceManager().getRetriever( Tape.class )
                            .attain( tape.getId() ).isFullOfData() );
            assertEqualsMod(
                    "Should notta removed any job entries since an exception occurred.",
                    2,
                    dbSupport.getDataManager().getCount( JobChunk.class, Require.nothing() ) );

            assertFalseMod(
                    "Should notta allowed task on this drive, since we've retried exhaustively on it.",
                    task.canUseTapeAlreadyInDrive(new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td1.getId() )) );

            mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
            mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            task.prepareForExecutionIfPossible(
                    tapeDriveResource,
                    new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td2.getId() ) );
            assertNotNullMod(
                    "Shoulda selected tape, indicating that task can be executed at this time.",
                    task.getTapeId() );

            TestUtil.invokeAndWaitUnchecked( task );

            updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
            assertEqualsMod(
                    "Shoulda notta updated original size of job.",
                    0,
                    updatedJob.getOriginalSizeInBytes() );
            assertEqualsMod(
                    "Should notta updated cached size of job.",
                    0,
                    updatedJob.getCachedSizeInBytes() );
            assertEqualsMod(
                    "Shoulda updated completed size of job.",
                    0,
                    updatedJob.getCompletedSizeInBytes() );
            assertEqualsMod(
                    "Should notta marked tape as full due to exception yet.",
                    false,
                    dbSupport.getServiceManager().getRetriever( Tape.class )
                            .attain( tape.getId() ).isFullOfData() );
            assertEqualsMod(
                    "Should notta removed any job entries since an exception occurred.",
                    2,
                    dbSupport.getDataManager().getCount( JobChunk.class, Require.nothing() ) );

            mockDaoDriver.updateBean(td1.setTapeId(null), TapeDrive.TAPE_ID);
            mockDaoDriver.updateBean(td2.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            task.prepareForExecutionIfPossible(
                    tapeDriveResource,
                    new MockTapeAvailability().setTapePartitionId( partitionId ).setDriveId( td2.getId() ) );
            assertNotNullMod(
                    "Shoulda selected tape, indicating that task can be executed at this time.",
                    task.getTapeId() );

            TestUtil.invokeAndWaitUnchecked( task );

            updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
            assertEqualsMod(
                    "Shoulda notta updated original size of job.",
                    0,
                    updatedJob.getOriginalSizeInBytes() );
            assertEqualsMod(
                    "Should notta updated cached size of job.",
                    0,
                    updatedJob.getCachedSizeInBytes() );
            assertEqualsMod(
                    "Shoulda updated completed size of job.",
                    0,
                    updatedJob.getCompletedSizeInBytes() );
            assertEqualsMod(
                    "Shoulda updated tape's state to BAD.",
                    TapeState.BAD,
                    dbSupport.getServiceManager().getRetriever( Tape.class )
                            .attain( tape.getId() ).getState() );
            assertEqualsMod(
                    "Should notta removed any job entries since an exception occurred.",
                    2,
                    dbSupport.getDataManager().getCount( JobChunk.class, Require.nothing() ) );
            assertFalseMod(
                    "Should notta marked successful completion of storage domain.",
                    persistenceTargetManager.getStorageDomainsContainingTape().isEmpty() );
            tapeFailureManagement.resetFailures(tape.getId(), td1.getId(), TapeFailureType.WRITE_FAILED);
            tapeFailureManagement.resetFailures(tape.getId(), td2.getId(), TapeFailureType.WRITE_FAILED);
        }


        cacheFilesystemDriver.shutdown();
    }*/


    @Test
    public void testRunWithRpcTimeoutExceptionWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            final Constructor< ? > con =
                                    RpcTimeoutException.class.getDeclaredConstructor( Exception.class );
                            con.setAccessible( true );
                            throw (RpcTimeoutException)con.newInstance(
                                    new RuntimeException( "Timed out." ) );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        final Job updatedJob = dbSupport.getServiceManager().getRetriever( Job.class ).attain( job.getId() );
        assertEquals(0,  updatedJob.getOriginalSizeInBytes(), "Shoulda notta updated original size of job.");
        assertEquals(0,  updatedJob.getCachedSizeInBytes(), "Should notta updated cached size of job.");
        assertEquals(0,  updatedJob.getCompletedSizeInBytes(), "Shoulda updated completed size of job.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class )
                .attain( tape.getId() ).isFullOfData(), "Should notta marked tape as full due to exception.");
        assertEquals(2,  dbSupport.getDataManager().getCount(JobEntry.class, Require.nothing()), "Should notta removed any job entries since an exception occurred.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta generated a tape failure.");
        assertFalse(mockDaoDriver.retrieveAll(LocalBlobDestination.class).stream().allMatch(
                        pt -> pt.getBlobStoreState() == JobChunkBlobStoreState.COMPLETED), "Should notta marked all persistence targets complete.");
        cacheFilesystemDriver.shutdown();
    }


    public void
    testRunWhereVerifyQuiescedToCheckpointReturnsNoModificationAndTapeHasLastCheckpointDoesNothing()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setLastCheckpoint( "original" ),
                Tape.LAST_CHECKPOINT );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        final Tape updatedTape =
                dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );
        assertEquals(TapeState.NORMAL, updatedTape.getState(), "Should notta changed tape state.");
        assertEquals("original", updatedTape.getLastCheckpoint(), "Should notta modified anything.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta modified anything.");
        cacheFilesystemDriver.shutdown();
    }


    public void
    testRunWhereVerifyQuiescedToCheckpointReturnsNoModificationAndTapeHasNoLastCheckpointDoesNothing()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );;
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        final Tape updatedTape =
                dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );
        assertEquals(TapeState.NORMAL, updatedTape.getState(), "Should notta changed tape state.");
        assertEquals("new", updatedTape.getLastCheckpoint(), "Should notta modified anything.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta modified anything.");
        cacheFilesystemDriver.shutdown();
    }


    public void
    testRunWhereVerifyQuiescedToCheckpointReturnsSuccessfulRollbackAndTapeHasLastCheckpointWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final UUID partitionId = mockDaoDriver.attainOneAndOnly(TapePartition.class).getId();
        final Tape tape = mockDaoDriver.createTape( partitionId, TapeState.NORMAL );
        final UUID td1 = mockDaoDriver.createTapeDrive( partitionId, "tdsn", tape.getId() ).getId();
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );




        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setLastCheckpoint( "original" ),
                Tape.LAST_CHECKPOINT );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setVerifyQuiescedToCheckpointResponse( "new" );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );
        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setDriveId( td1 ).setTapePartitionId( partitionId ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );

        TestUtil.invokeAndWaitUnchecked( task );

        final Tape updatedTape =
                dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );
        assertEquals(TapeState.NORMAL, updatedTape.getState(), "Should notta changed tape state.");
        assertEquals("new", updatedTape.getLastCheckpoint(), "Shoulda updated checkpoint.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta modified anything.");
        cacheFilesystemDriver.shutdown();
    }


    public void
    testRunWhereVerifyQuiescedToCheckpointThrowsInternalExceptionAndTapeHasLastCheckpointWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        //mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );

        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setLastCheckpoint( "original" ),
                Tape.LAST_CHECKPOINT );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setVerifyQuiescedToCheckpointException(
                new RpcProxyException( "", BeanFactory.newBean( Failure.class ) ) );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        final Tape updatedTape =
                dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );
        assertEquals("original", updatedTape.getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, updatedTape.getState(), "Shoulda updated tape state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta updated state for data loss.");
        cacheFilesystemDriver.shutdown();
    }


    public void
    testRunWhereVerifyQuiescedToCheckpointThrowsCheckpointNotFoundAndTapeHasLastCheckpointWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o1 = mockDaoDriver.createObject( null, "o1", 10 );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob1, blob2 ) );
        //mockDaoDriver.putBlobOnTape( tape.getId(), blob2.getId() );

        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setLastCheckpoint( "original" ),
                Tape.LAST_CHECKPOINT );

        final MockCacheFilesystemDriver cacheFilesystemDriver = new MockCacheFilesystemDriver( dbSupport );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setVerifyQuiescedToCheckpointException(
                new RpcProxyException( "", BeanFactory.newBean( Failure.class ).setCode(
                        TapeResourceFailureCode.CHECKPOINT_NOT_FOUND.toString() ) ) );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setInvocationListener( InterfaceProxyFactory.getProxy(
                TapeDriveResource.class,
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeDriveResource.class, "writeData" ), ( proxy, method, args ) -> {
                            throw new RpcProxyException( "", BeanFactory.newBean( Failure.class ) );
                        },
                        null ) ) );


        cacheManager.allocateChunksForBlob( blob1.getId() );
        cacheManager.allocateChunksForBlob( blob2.getId() );
        
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );
        task.prepareForExecutionIfPossible(
                tapeDriveResource,
                new MockTapeAvailability().setTapePartitionId( tape.getPartitionId() ) );
        assertNotNull(task.getTapeId(), "Shoulda selected tape, indicating that task can be executed at this time.");
        cacheFilesystemDriver.writeCacheFile( blob1.getId(), 10 );
        cacheFilesystemDriver.writeCacheFile( blob2.getId(), 20 );
        cacheManager.blobLoadedToCache( blob1.getId() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        final Tape updatedTape =
                dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );
        assertEquals("original", updatedTape.getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_MISSING, updatedTape.getState(), "Shoulda updated tape state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should notta updated state for data loss.");
        cacheFilesystemDriver.shutdown();
    }


    @Test
    public void testCanUseAvailableTapeDoesNotAssignToStorageDomain()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final Tape tape = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO6 );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly(TapePartition.class);

        final MockDiskManager cacheManager = new MockDiskManager( mockDaoDriver.getServiceManager() );
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );

        final TapeAvailability mockAvailability = mock(TapeAvailability.class);
        when(mockAvailability.getAllUnavailableTapes()).thenReturn(null);
        when(mockAvailability.getTapePartitionId()).thenReturn(partition.getId());
        when(mockAvailability.getTapeInDrive()).thenReturn(null);

        assertTrue(task.canUseAvailableTape( mockAvailability ), "Should be able to use available tape");

        assertFalse(dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() ).isAssignedToStorageDomain(), "tape was not assigned to storage domain");

        final Tape dbTape = dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );

        assertNull( dbTape.getBucketId(), "Should not have set bucket id");
        assertNull( dbTape.getStorageDomainMemberId(), "Should not have set storage domain member");
        assertFalse(dbTape.isAssignedToStorageDomain(), "Should not have assigned to storage domain");
    }


    @Test
    public void testCanUseTapeAlreadyInDriveDoesNotAssignToStorageDomain()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createABMConfigSingleCopyOnTape();
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );


        final Set<JobEntry> chunks = mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );

        final Tape tape = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO6 );
        final TapePartition partition = mockDaoDriver.attainOneAndOnly(TapePartition.class);

        final MockDiskManager cacheManager = new MockDiskManager( mockDaoDriver.getServiceManager() );
        final WriteChunkToTapeTask task = createTask(chunks, getTapeEjector(), cacheManager, mockDaoDriver );

        final TapeAvailability mockAvailability = mock(TapeAvailability.class);
        when(mockAvailability.getAllUnavailableTapes()).thenReturn(null);
        when(mockAvailability.getTapePartitionId()).thenReturn(partition.getId());
        when(mockAvailability.getTapeInDrive()).thenReturn(tape.getId());

        assertTrue(task.canUseTapeAlreadyInDrive( mockAvailability ), "Should be able to use available tape");

        assertFalse(dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() ).isAssignedToStorageDomain(), "tape was not assigned to storage domain");

        final Tape dbTape = dbSupport.getServiceManager().getService( TapeService.class ).attain( tape.getId() );

        assertNull( dbTape.getBucketId(), "Should not have set bucket id");
        assertNull( dbTape.getStorageDomainMemberId(), "Should not have set storage domain member");
        assertFalse(dbTape.isAssignedToStorageDomain(), "Should not have assigned to storage domain");
    }


    private TapeEjector getTapeEjector( final BasicTestsInvocationHandler btih )
    {
        return InterfaceProxyFactory.getProxy( TapeEjector.class, btih );
    }


    private TapeEjector getTapeEjector()
    {
        return getTapeEjector( null );
    }


    private WriteChunkToTapeTask createTask(Collection<JobEntry> chunks, TapeEjector tapeEjector, DiskManager cacheManager, MockDaoDriver mockDaoDriver) {
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        return new WriteChunkToTapeTask(
                BlobStoreTaskPriority.values()[ 0 ],
                pts,
                tapeEjector,
                cacheManager,
                new JobProgressManagerImpl( mockDaoDriver.getServiceManager(), BufferProgressUpdates.NO ),
                new TapeFailureManagement(mockDaoDriver.getServiceManager()),
                mockDaoDriver.getServiceManager());
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