/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import com.spectralogic.s3.common.dao.domain.pool.*;
import com.spectralogic.s3.common.dao.orm.S3ObjectRM;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.dataplanner.backend.frmwrk.LocalWriteDirective;
import com.spectralogic.s3.dataplanner.backend.pool.api.*;
import com.spectralogic.s3.dataplanner.testfrmwrk.PoolTaskBuilder;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl.BufferProgressUpdates;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.PoolLockSupportImpl;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.cache.DiskManagerImpl;
import com.spectralogic.s3.dataplanner.cache.MockTierExistingCacheImpl;
import com.spectralogic.s3.dataplanner.testfrmwrk.MockPoolPersistence;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class WriteChunkToPoolTask_Test
{
    @Test
    public void testHappyConstruction()
    {
        new PoolTaskBuilder(dbSupport.getServiceManager())
                .buildWriteTask(
                new LocalWriteDirective(
                        new HashSet<>(),
                        BeanFactory.newBean(StorageDomain.class),
                        BlobStoreTaskPriority.values()[ 0 ],
                        new HashSet<>(),
                        0,
                        BeanFactory.newBean(Bucket.class))
        );
    }
    

    @Test
    public void testPrepareForExecutionSelectsNoPoolIfSelectionNotPossible()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createPool( partition.getId(), PoolState.FOREIGN );
        mockDaoDriver.createPool( partition.getId(), PoolState.LOST );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob.getLength(),
                        bucket)
        );


        task.prepareForExecutionIfPossible();
        assertEquals(
                null,
                task.getPoolId(),
                "Should notta selected non-normal-state pool."
                );
        assertEquals(
                 BlobStoreTaskState.READY,
                task.getState(),
                "Shoulda reported ready to run to retry."
                );
    }
    
    @Test
    public void testPrepareForExecutionSelectsNoPoolIfCannotTakeLockAgainstPool()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Pool pool1 = mockDaoDriver.createPool( partition.getId(), null );
        mockDaoDriver.createPool( partition.getId(), PoolState.LOST );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob.getLength(),
                        bucket)
        );
        lockSupport.acquireExclusiveLock( 
                pool1.getId(),
                InterfaceProxyFactory.getProxy( PoolTask.class, null ) );

        task.prepareForExecutionIfPossible();
        assertEquals(
                null,
                task.getPoolId(),
                "Should notta selected unlockable pool."
                 );
        assertEquals(
                BlobStoreTaskState.READY,
                task.getState(),
                "Shoulda reported ready to run to retry."
                 );
    }
    
    @Test
    public void testPrepareForExecutionWhenJobChunkNoLongerExistsNotPossible()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createPool( partition.getId(), null );
        mockDaoDriver.createPool( partition.getId(), PoolState.LOST );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob.getLength(),
                        bucket)
        );


        mockDaoDriver.delete(JobEntry.class, chunks);
        task.prepareForExecutionIfPossible();
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completed to not retry.");
    }
    
    @Test
    public void testPrepareForExecutionSelectsAndLocksPoolIfSelectionPossible()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Pool pool1 = mockDaoDriver.createPool( partition.getId(), null );
        mockDaoDriver.createPool( partition.getId(), PoolState.LOST );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob.getLength(),
                        bucket)
        );

        
        task.prepareForExecutionIfPossible();
        assertEquals(
                pool1.getId(),
                task.getPoolId(),
                "Shoulda selected normal-state pool."
                );
        assertEquals(
                BlobStoreTaskState.PENDING_EXECUTION,
                task.getState(),
                "Shoulda reported ready to execute."
                );
        assertEquals(
                CollectionFactory.toSet( pool1.getId() ),
                lockSupport.getPoolsUnavailableForExclusiveLock(),
                "Shoulda locked selected pool."
                );
        assertEquals(
                CollectionFactory.toSet(),
                lockSupport.getPoolsUnavailableForWriteLock(),
                "Should not prevent concurrent write locks."
                 );
    }
    
    @Test
    public void testPrepareForExecutionWhenLockingFailureDoesNotInvalidateTask()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createPool( partition.getId(), null );
        mockDaoDriver.createPool( partition.getId(), PoolState.LOST );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > properLockSupport = createLockSupport();
        @SuppressWarnings( "unchecked" ) final PoolLockSupport< PoolTask > lockSupport =
                InterfaceProxyFactory.getProxy( PoolLockSupport.class, ( proxy, method, args ) -> {
                    if ( method.getName()
                            .contains( "acquire" ) )
                    {
                        throw new PoolLockingException( "Can't lock at this time.",
                                new IllegalStateException( "Cannot acquire lock." ) );
                    }
                    return method.invoke( properLockSupport, args );
                } );
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob.getLength(),
                        bucket)
        );
        
        task.prepareForExecutionIfPossible();
        assertEquals(
                BlobStoreTaskState.READY,
                task.getState(),
                "Shoulda reported not ready to execute.");
    }
    
    @Test
    public void testRunResultsInDateLastVerifiedInvalidated()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        mockDaoDriver.updateBean( mockPoolPersistence.getPool()
                                                     .setLastVerified( new Date() ), PersistenceTarget.LAST_VERIFIED );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
    
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket)
        );
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
    
        assertNull(
                mockDaoDriver.attain( mockPoolPersistence.getPool() )
                        .getLastVerified(),
                "Shoulda invalidated date last verified by writing to it."
                );
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testRunWithFailureToWriteFileResultsInRetry() throws IOException
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.updateBean( blob1.setChecksum( "v" )
                                       .setChecksumType( ChecksumType.values()[ 0 ] ), ChecksumObservable.CHECKSUM,
                ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.updateBean( blob2.setChecksum( "v" )
                                       .setChecksumType( ChecksumType.values()[ 0 ] ), ChecksumObservable.CHECKSUM,
                ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
    
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket)
        );
    
        /*
         * Write out a single blob file and no other supporting files.
         * This will cause a condition where the bucket directory is not valid, but not empty,
         * which is a RuntimeError
         */
        final Path fileBlob1 =
                PoolUtils.getPath( mockPoolPersistence.getPool(), bucket.getName(), o1.getId(), blob1.getId() );
        Files.createDirectories( fileBlob1.getParent() );
        Files.createFile( fileBlob1 );
        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertEquals( BlobStoreTaskState.READY, task.getState(), "Shoulda reported ready due to write error." );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                .getRetriever( PoolFailure.class )
                .getCount(),
                "Shoulda generated pool failure.");
        lockSupport.releaseLock( task );
    
        new ThreadedTrashCollector().emptyTrash(
                PoolUtils.getPath( mockPoolPersistence.getPool(), bucket.getName(), null, null )
                         .getParent() );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
    
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(
                task.getState(),
                BlobStoreTaskState.COMPLETED,
                "Shoulda reported completion."
                 );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( PoolFailure.class )
                        .getCount(),
                "Should notta generated pool failure."  );
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testRunWithInvalidPoolSourceResultsInEventualSuspectBlobPools() throws IOException
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );

        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1 ) );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        mockDaoDriver.updateBean( mockPoolPersistence.getPool()
                .setLastVerified( new Date() ), PersistenceTarget.LAST_VERIFIED );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool(mockDaoDriver.createPool(partition2.getId(), null).getId(), blob1.getId());
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke(final Object proxy, final Method method, final Object[] args )
                    {
                        return BeanFactory.newBean( DiskFileInfo.class )
                                .setFilePath( "/some/file.txt" )
                                .setBlobPoolId(blobPool.getId());
                    }
                } );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ), poolBlobStore );

        final RetrieveBeansResult<S3ObjectProperty> metadata = new S3ObjectRM(o1.getId(), dbSupport.getServiceManager()).getS3ObjectProperties();
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength(),
                        bucket)
        );

        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        for (int i = 0; i < 3; i++) {
            assertEquals(
                    0,
                    dbSupport.getServiceManager()
                            .getRetriever( SuspectBlobPool.class )
                            .getCount(),
                    "Should be no suspect blobs yet."
                    );
            assertEquals(
                    BlobStoreTaskState.READY,
                    task.getState(),
                    "Shoulda reported ready due to write error."  );
            task.prepareForExecutionIfPossible();
            TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    TestUtil.invokeAndWaitChecked( task );
                }
            } );
            assertEquals(
                    i + 1,
                    dbSupport.getServiceManager()
                            .getRetriever( PoolFailure.class ).getCount(),
                    "Shoulda generated pool failure."  );
            lockSupport.releaseLock( task );
        }
        assertEquals(
                BlobStoreTaskState.NOT_READY,
                task.getState(),
                "Shoulda reported ready due to write error." );
        assertEquals(
                blobPool.getId(),
                mockDaoDriver.attainOneAndOnly(SuspectBlobPool.class).getId(),
                "Should be exactly one suspect blob matching our blob pool."
                 );

        new ThreadedTrashCollector().emptyTrash(
                PoolUtils.getPath( mockPoolPersistence.getPool(), bucket.getName(), null, null )
                        .getParent() );

        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testPoolReadFailureDuringIomMigrationMarksDataMigrationInError() throws IOException
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.updateBean(
                dbSupport.getServiceManager().getRetriever(Job.class).attain(job.getId())
                        .setIomType(IomType.STANDARD_IOM),
                Job.IOM_TYPE);
        final JobEntry chunk = mockDaoDriver.createJobEntry(job.getId(), blob1 );

        final DataMigration migration = BeanFactory.newBean(DataMigration.class)
                .setPutJobId(job.getId());
        dbSupport.getServiceManager().getCreator(DataMigration.class).create(migration);

        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        mockDaoDriver.updateBean( mockPoolPersistence.getPool()
                .setLastVerified( new Date() ), PersistenceTarget.LAST_VERIFIED );
        final BlobPool blobPool = mockDaoDriver.putBlobOnPool(mockDaoDriver.createPool(partition2.getId(), null).getId(), blob1.getId());
        final PoolBlobStore poolBlobStore = InterfaceProxyFactory.getProxy(
                PoolBlobStore.class,
                new InvocationHandler()
                {
                    public Object invoke(final Object proxy, final Method method, final Object[] args )
                    {
                        return BeanFactory.newBean( DiskFileInfo.class )
                                .setFilePath( "/some/file.txt" )
                                .setBlobPoolId(blobPool.getId());
                    }
                } );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ), poolBlobStore );

        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(Set.of(chunk));
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        Set.of(chunk),
                        blob1.getLength(),
                        bucket)
        );

        assertFalse(dbSupport.getServiceManager().getRetriever(DataMigration.class)
                        .attain(migration.getId()).isInError(),
                "Migration should not be in error yet.");

        for (int i = 0; i < 3; i++) {
            task.prepareForExecutionIfPossible();
            TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    TestUtil.invokeAndWaitChecked( task );
                }
            } );
            lockSupport.releaseLock( task );
        }

        assertEquals(
                blobPool.getId(),
                mockDaoDriver.attainOneAndOnly(SuspectBlobPool.class).getId(),
                "Should be exactly one suspect blob matching our blob pool.");
        assertTrue(dbSupport.getServiceManager().getRetriever(DataMigration.class)
                        .attain(migration.getId()).isInError(),
                "Data migration should be marked in error after pool read failures.");

        new ThreadedTrashCollector().emptyTrash(
                PoolUtils.getPath( mockPoolPersistence.getPool(), bucket.getName(), null, null )
                        .getParent() );

        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testRunWithMultiplePersistenceTargetsWritesOnlyToCorrectTarget()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain2.getId(), partition2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule( dataPolicy.getId(), DataPersistenceRuleType.PERMANENT,
                storageDomain2.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final MockPoolPersistence mockPoolPersistence2 = new MockPoolPersistence( dbSupport, partition2.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
    
        final Pool pool1 = mockPoolPersistence.getPool();
        final Pool pool2 = mockPoolPersistence2.getPool();
        mockDaoDriver.updateBean( pool2.setState( PoolState.LOST ), Pool.STATE );
    
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                        new LocalWriteDirective(
                                pts,
                                storageDomain,
                                BlobStoreTaskPriority.values()[ 0 ],
                                chunks,
                                blob1.getLength() + blob2.getLength(),
                                bucket)
                );

        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        mockPoolPersistence2.assertBlobsPersisted( new HashSet<>() );
        task.prepareForExecutionIfPossible();
        assertEquals( pool1.getId(), task.getPoolId(), "Shoulda selected correct pool to write to." );

        TestUtil.invokeAndWaitUnchecked( task );

        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        mockPoolPersistence2.assertBlobsPersisted( new HashSet<>() );
        assertEquals( BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completion." );
        assertNotNull( mockDaoDriver.attain( pool1 ).getLastAccessed(), "Shoulda updated date last accessed." );
        assertNotNull( mockDaoDriver.attain( pool1 ).getLastModified(), "Shoulda updated date last modified." );
        assertNull( mockDaoDriver.attain( pool2 ).getLastAccessed(), "Should notta updated date last accessed." );
        assertNull( mockDaoDriver.attain( pool2 ).getLastModified(), "Should notta updated date last modified." );
        mockPoolPersistence.shutdown();
        mockPoolPersistence2.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }
    
    @Test
    public void testRunWithEntriesDeletedGetsInvalidated()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
    
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket)
        );
    
        dbSupport.getDataManager()
                 .deleteBeans( JobEntry.class, Require.nothing() );
        task.prepareForExecutionIfPossible();
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completion." );
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }
    
    @Test
    public void testRunWithSinglePersistenceTargetWritesChunkToPool()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.updateBean( o1.setCreationDate( new Date( 10002 ) ), S3Object.CREATION_DATE );
        final List < S3ObjectProperty > o1Properties = new ArrayList<>();
        o1Properties.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "foo" )
                .setValue( "bar" ) );
        dbSupport.getServiceManager().getService( S3ObjectPropertyService.class )
        .createProperties( o1.getId(), o1Properties );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
    
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket)
        );
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completion."
                );
        assertEquals(
                2,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Shoulda created blob pool records."
                );
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testStageJobsOnlyUpdateIfBlobAlreadyOnPool()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.updateBean( o1.setCreationDate( new Date( 10002 ) ), S3Object.CREATION_DATE );
        final List < S3ObjectProperty > o1Properties = new ArrayList<>();
        o1Properties.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "foo" )
                .setValue( "bar" ) );
        dbSupport.getServiceManager().getService( S3ObjectPropertyService.class )
                .createProperties( o1.getId(), o1Properties );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );

        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        final UUID jobId = chunks.iterator().next().getJobId();
        final Job job = mockDaoDriver.attain( Job.class, jobId );
        //make this a stage job
        mockDaoDriver.updateBean( job.setIomType( IomType.STAGE), Job.IOM_TYPE);

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final Pool pool = mockPoolPersistence.getPool();
        final BlobPool originalBlobPool = mockDaoDriver.putBlobOnPool(pool.getId(), blob2.getId());

        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket,
                        true,
                        false)
        );

        mockPoolPersistence.create(blob2);
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob2 ) );

        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Should have only one blob pool record so far."
                );

        task.prepareForExecutionIfPossible();

        TestUtil.invokeAndWaitUnchecked( task );

        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completion."
                 );
        assertEquals(
                2,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Should have two blob pool records."
                );

        //get updated blobPool
        final BlobPool updatedBlobPool = mockDaoDriver.attain( BlobPool.class, originalBlobPool.getId() );
        //make sure the new dates are later than the old ones
        assertTrue(
                updatedBlobPool.getLastAccessed().after( originalBlobPool.getLastAccessed() ),
                "Shoulda updated last accessed date."  );
        assertTrue(
                updatedBlobPool.getDateWritten().after( originalBlobPool.getDateWritten() ),
                "Shoulda updated date written." );
        assertEquals(
                 originalBlobPool.getPoolId(),
                updatedBlobPool.getPoolId(),
                "Should notta changed pool id.");
        assertEquals(
                originalBlobPool.getBlobId(),
                updatedBlobPool.getBlobId(),
                "Should notta changed blob id.");

        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    @Test
    public void testStageJobsOnlyUpdateIfBlobAlreadyOnStorageDomainButDifferentPool()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.updateBean( o1.setCreationDate( new Date( 10002 ) ), S3Object.CREATION_DATE );
        final List < S3ObjectProperty > o1Properties = new ArrayList<>();
        o1Properties.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "foo" )
                .setValue( "bar" ) );
        dbSupport.getServiceManager().getService( S3ObjectPropertyService.class )
                .createProperties( o1.getId(), o1Properties );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );

        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        final UUID jobId = chunks.iterator().next().getJobId();
        final Job job = mockDaoDriver.attain( Job.class, jobId );
        //make this a stage job
        mockDaoDriver.updateBean( job.setIomType( IomType.STAGE), Job.IOM_TYPE);

        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(storageDomain.setWriteOptimization(WriteOptimization.PERFORMANCE), StorageDomain.WRITE_OPTIMIZATION);
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence1 =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final MockPoolPersistence mockPoolPersistence2 =
                new MockPoolPersistence( dbSupport, partition.getId() );

        final Pool pool1 = mockPoolPersistence1.getPool();
        final Pool pool2 = mockPoolPersistence2.getPool();
        final BlobPool originalBlobPool = mockDaoDriver.putBlobOnPool(pool2.getId(), blob2.getId());
        final StorageDomainMember sdm = mockDaoDriver.attainOneAndOnly(StorageDomainMember.class);
        mockDaoDriver.updateBean(pool2.setStorageDomainMemberId(sdm.getId()).setAssignedToStorageDomain(true),
                Pool.STORAGE_DOMAIN_MEMBER_ID, Pool.ASSIGNED_TO_STORAGE_DOMAIN);

        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );

        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);

        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket,
                        true,
                        false)
        );
        //lock pool2 to make sure we select pool 1
        lockSupport.acquireExclusiveLock(pool2.getId(), InterfaceProxyFactory.getProxy(PoolTask.class, null));

        mockPoolPersistence2.create(blob2);
        mockPoolPersistence1.assertBlobsPersisted( CollectionFactory.toSet() );
        mockPoolPersistence2.assertBlobsPersisted( CollectionFactory.toSet( blob2 ) );

        assertEquals(
                1,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Should have only one blob pool record so far."
                );

        task.prepareForExecutionIfPossible();

        TestUtil.invokeAndWaitUnchecked( task );

        mockPoolPersistence1.assertBlobsPersisted( CollectionFactory.toSet( blob1 ) );
        mockPoolPersistence2.assertBlobsPersisted( CollectionFactory.toSet( blob2 ) );
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completion."
                 );
        assertEquals(
                2,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Should have two blob pool records."
                 );

        //get updated blobPool
        final BlobPool updatedBlobPool = mockDaoDriver.attain( BlobPool.class, originalBlobPool.getId() );
        //make sure the new dates are later than the old ones
        assertTrue(
                updatedBlobPool.getLastAccessed().after( originalBlobPool.getLastAccessed() ),
                "Shoulda updated last accessed date."  );
        assertTrue(
                updatedBlobPool.getDateWritten().after( originalBlobPool.getDateWritten() ) ,
                "Shoulda updated date written." );
        assertEquals(
                originalBlobPool.getPoolId(),
                updatedBlobPool.getPoolId(),
                "Should notta changed pool id." );
        assertEquals(
                originalBlobPool.getBlobId(),
                updatedBlobPool.getBlobId(),
                "Should notta changed blob id.");
        //make sure blob 1 is on pool 1
        final BlobPool blobPool1 = dbSupport.getServiceManager().getRetriever( BlobPool.class ).attain(Require.beanPropertyEquals(BlobPool.BLOB_ID, blob1.getId()));
        assertEquals(
                pool1.getId(),
                blobPool1.getPoolId(),
                "Should be on pool 1" );

        mockPoolPersistence1.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }
    
    @Test
    public void testRunWithVerifyAfterWriteFailsWhenVerifyFailures()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.updateBean( o1.setCreationDate( new Date( 10002 ) ), S3Object.CREATION_DATE );
        final List < S3ObjectProperty > o1Properties = new ArrayList<>();
        o1Properties.add( BeanFactory.newBean( S3ObjectProperty.class )
                .setKey( "foo" )
                .setValue( "bar" ) );
        dbSupport.getServiceManager().getService( S3ObjectPropertyService.class )
        .createProperties( o1.getId(), o1Properties );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Job.class )
                                                 .setVerifyAfterWrite( true ), Job.VERIFY_AFTER_WRITE );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
    
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket,
                        false,
                        true)
        );
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        task.prepareForExecutionIfPossible();
    
        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitUnchecked( task ) );
        
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(
                BlobStoreTaskState.READY,
                task.getState(),
                "Should notta reported completion."  );
        assertEquals(
                0,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Should notta created blob pool records." );
        assertEquals(
                1,
                dbSupport.getServiceManager()
                        .getRetriever( PoolFailure.class )
                        .getCount(),
                "Shoulda reported failure."  );
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }
    
    @Test
    public void testRunWithVerifyAfterWriteWritesChunkToPoolWhenNoVerifyFailures()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.updateBean( o1.setCreationDate( new Date( 10002 ) ), S3Object.CREATION_DATE );
        final List< S3ObjectProperty > o1Properties = new ArrayList<>();
        o1Properties.add( BeanFactory.newBean( S3ObjectProperty.class )
                                     .setKey( "foo" )
                                     .setValue( "bar" ) );
        dbSupport.getServiceManager()
                 .getService( S3ObjectPropertyService.class )
                 .createProperties( o1.getId(), o1Properties );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Job.class )
                                                 .setVerifyAfterWrite( true ), Job.VERIFY_AFTER_WRITE );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver =
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence =
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
    
        mockDaoDriver.updateAllBeans( BeanFactory.newBean( Blob.class )
                                                 .setChecksum( "44podg==" ), ChecksumObservable.CHECKSUM );
        mockDaoDriver.attainAndUpdate( Blob.class, blob1 );
        mockDaoDriver.attainAndUpdate( Blob.class, blob2 );

        final JobProgressManager jobProgressManager = new JobProgressManagerImpl( dbSupport.getServiceManager(),
                BufferProgressUpdates.NO );
    
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withJobProgressManager(jobProgressManager)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket)
        );
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completion.");
        assertEquals(2,  dbSupport.getServiceManager()
                .getRetriever(BlobPool.class)
                .getCount(), "Shoulda created blob pool records.");
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }
    
    @Test
    public void testWriteChunkSucceedsIfInvalidBucketDirExists()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final Set<JobEntry> chunks =
                mockDaoDriver.createJobWithEntries( JobRequestType.PUT, CollectionFactory.toSet( blob1, blob2 ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence = 
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(chunks);

        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[ 0 ],
                        chunks,
                        blob1.getLength() + blob2.getLength(),
                        bucket)
        );
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        final Path bucketDir = PoolUtils.getPath( mockPoolPersistence.getPool(), bucket.getName(), null, null );
        try
        {
            Files.createDirectories( bucketDir );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to delete: " + bucketDir, ex );
        }
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completion."
                 );
        assertEquals(
                2,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Shoulda created blob pool records."
                );
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }
    
    @Test
    public void testWriteChunkSucceedsIfInvalidObjectDirExists() throws IOException
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        
        final JobEntry chunk1 =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT,  blob1 );
        final JobEntry chunk2 =
                mockDaoDriver.createJobWithEntry( JobRequestType.PUT,  blob2 );
        
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final MockCacheFilesystemDriver mockCacheFilesystemDriver = 
                new MockCacheFilesystemDriver( dbSupport );
        final MockPoolPersistence mockPoolPersistence = 
                new MockPoolPersistence( dbSupport, partition.getId() );
        final DiskManager cacheManager = new DiskManagerImpl( new CacheManagerImpl( dbSupport.getServiceManager(),
                new MockTierExistingCacheImpl() ) );
        
        cacheManager.allocateChunksForBlob( blob1.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob1.getId(), blob1.getLength() );
        cacheManager.blobLoadedToCache( blob1.getId() );
        blob1.setChecksum( "foo" );
        blob1.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        cacheManager.allocateChunksForBlob( blob2.getId() );
        mockCacheFilesystemDriver.writeCacheFile( blob2.getId(), blob2.getLength() );
        cacheManager.blobLoadedToCache( blob2.getId() );
        blob2.setChecksum( "foo" );
        blob2.setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk1.getId());
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final WriteChunkToPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport( lockSupport )
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts,
                        storageDomain,
                        BlobStoreTaskPriority.values()[0],
                        Set.of(chunk1),
                        blob1.getLength(),
                        bucket)
        );
    
        mockPoolPersistence.assertBlobsPersisted( new HashSet<>() );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1 ) );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda reported completion.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda created blob pool records.");
        lockSupport.releaseLock( task );

        final Set<LocalBlobDestination> pts2 = mockDaoDriver.createPersistenceTargetsForChunk(chunk2.getId());
        final WriteChunkToPoolTask task2 = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .withDiskManager(cacheManager)
                .buildWriteTask(
                new LocalWriteDirective(
                        pts2,
                        storageDomain,
                        BlobStoreTaskPriority.values()[0],
                        Set.of(chunk2),
                        blob2.getLength(),
                        bucket)
        );
    
        final Path objectDir = PoolUtils.getPath( mockPoolPersistence.getPool(), bucket.getName(), o2.getId(), null );
        Files.createDirectories( objectDir );
        
        task2.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task2 );
        
        mockPoolPersistence.assertBlobsPersisted( CollectionFactory.toSet( blob1, blob2 ) );
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                task.getState(),
                "Shoulda reported completion."
                 );
        assertEquals(
                2,
                dbSupport.getServiceManager().getRetriever( BlobPool.class ).getCount(),
                "Shoulda created blob pool records."
                 );
        
        
        mockPoolPersistence.shutdown();
        mockCacheFilesystemDriver.shutdown();
    }

    private PoolLockSupport< PoolTask > createLockSupport()
    {
        return new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }

    final DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

}
