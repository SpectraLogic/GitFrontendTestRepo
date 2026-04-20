/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.dataplanner.testfrmwrk.PoolTaskBuilder;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.PoolLockSupportImpl;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.s3.dataplanner.testfrmwrk.MockPoolPersistence;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class VerifyPoolTask_Test 
{

    @Test
    public void testConstructorNullPoolIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

        public void test()
            {
                new PoolTaskBuilder(null).buildVerifyPoolTask(
                        BlobStoreTaskPriority.values()[ 0 ],
                        null, null);
            }
        } );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        
        new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                pool.getId(), dbSupport.getServiceManager());
    }
    
    
    @Test
    public void testPrepareForExecutionWhenDesiredPoolLockedDoesNotTransistToPendingExecution()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final UUID poolId = pool.getId();
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        lockSupport.acquireExclusiveLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ) );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Should notta prepared for execution.");
        assertEquals(null, task.getPoolId(), "Should notta prepared for execution.");
    }
    
    
    @Test
    public void testPrepareForExecutionWhenDesiredPoolAvailableToLockTransistsToPendingExecution()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final UUID poolId = pool.getId();
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda prepared for execution.");
        assertEquals(poolId, task.getPoolId(), "Shoulda prepared for execution.");
    }

    
    @Test
    public void testRunWhenFailureResultsInPoolVerificationFailureGenerated()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final UUID poolId = pool.getId();

        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );

        final BasicTestsInvocationHandler btih =
                new BasicTestsInvocationHandler( MockInvocationHandler.forMethod( 
                        ReflectUtil.getMethod( PoolEnvironmentResource.class, "verifyPool" ),
                        new ConstantResponseInvocationHandler( "oops" ),
                        null ) );
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withPoolEnvironmentResource(getPoolEnvironmentResource( btih ))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date initially.");
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.invokeAndWaitChecked( task );
            }
        } );
        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta updated last verified date.");
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda marked as needing to run again.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda generated verification failure.");
    }
    
    
    @Test
    public void testRunWhenNothingToVerifyResultsInImmediateVerification()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool = mockDaoDriver.createPool();
        final UUID poolId = pool.getId();
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date initially.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Should notta generated verification failure.");
    }
    
    
    @Test
    public void testRunWhenSomethingToVerifyCanVerifyInSingleRoundResultsInVerification()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b3.getId() );
        poolPersistence.create( b1, b2, b3 );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date initially.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda required another round.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda retained all blob pool records.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Should notta generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWhenSomethingToVerifyCannotVerifyInSingleRoundResultsInVerification()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b3.getId() );
        poolPersistence.create( b1, b2, b3 );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );

        final BasicTestsInvocationHandler poolEnvironmentResourceBtih = getPoolEnvironmentResourceBtih();
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withPoolEnvironmentResource(getPoolEnvironmentResource( poolEnvironmentResourceBtih ))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId,
                2);
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date initially.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another round.");
        assertEquals(1,  poolEnvironmentResourceBtih.getTotalCallCount(), "Shoulda invoked verify pool.");
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda required another round.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda retained all blob pool records.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Should notta generated verification failure.");
        assertEquals(1,  poolEnvironmentResourceBtih.getTotalCallCount(), "Should notta invoked verify pool again.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWhenSomethingToVerifyCannotVerifyInSingleRoundAndInterruptedResultsInVerification()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b3.getId() );
        poolPersistence.create( b1, b2, b3 );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId,
                2);
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date initially.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another round.");
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.sleep( 10 );
        mockDaoDriver.updateBean( pool.setLastModified( new Date() ), PersistenceTarget.LAST_MODIFIED );
        TestUtil.sleep( 10 );

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another round.");
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another round.");
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda retained all blob pool records.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Should notta generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWithBlobVerificationFailureDueToBlobFileMissingResultsInDataLossRecorded()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o3.getId(), 2, 3 );
        blobs.get( 0 ).setChecksum( "CFSJfw==" );
        blobs.get( 0 ).setChecksumType( ChecksumType.CRC_32 );
        blobs.get( 1 ).setChecksum( "CFSJfw==" );
        blobs.get( 1 ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( 
                blobs.get( 0 ), ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        mockDaoDriver.updateBean( 
                blobs.get( 1 ), ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 0 ).getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 1 ).getId() );
        poolPersistence.create( b1, b2, blobs.get( 0 ) );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda recorded blob suspect.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Should notta whacked recorded blob for being suspect.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWithBlobVerificationFailureDueToBlobFileLengthMismatchResultsInDataLossRecorded()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b3.getId() );
        poolPersistence.create( b1, b2 );
        poolPersistence.create( b3.setLength( 99 ) );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda recorded suspect blob.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWithBlobVerificationFailureDueToObjectHashFolderMissingResultsInDataLossRecorded()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o3.getId(), 2, 3 );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 0 ).getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 1 ).getId() );
        poolPersistence.create( b1, b2 );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda recorded suspect blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWithBlobVerificationFailureDueToObjectFolderMissingResultsInDataLossRecorded() 
            throws IOException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o3.getId(), 2, 3 );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 0 ).getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 1 ).getId() );
        poolPersistence.create( b1, b2 );
        new File( PoolUtils.getPath( pool, bucket2.getName(), null, null )
                  + Platform.FILE_SEPARATOR + o3.getId().toString().substring( 0, 2 ) ).createNewFile();
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(BlobPool.class).getCount(), "Shoulda recorded suspect blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    @Test
    public void testRunWithBlobVerificationFailureDueToBucketFolderMissingResultsInDataLossRecorded()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "RWzXRg==" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( o3.getId(), 2, 3 );
        
        final MockPoolPersistence poolPersistence = new MockPoolPersistence( dbSupport, null );
        final Pool pool = poolPersistence.getPool();
        final UUID poolId = pool.getId();
        mockDaoDriver.putBlobOnPool( poolId, b1.getId() );
        mockDaoDriver.putBlobOnPool( poolId, b2.getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 0 ).getId() );
        mockDaoDriver.putBlobOnPool( poolId, blobs.get( 1 ).getId() );
        poolPersistence.create( b1 );
        
        lockSupport.acquireWriteLock( poolId, InterfaceProxyFactory.getProxy( PoolTask.class, null ), 10, 100 );
        
        final VerifyPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withPoolEnvironmentResource(getPoolEnvironmentResource(null))
                .withLockSupport(lockSupport)
                .buildVerifyPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                poolId, dbSupport.getServiceManager());
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();

        assertNull(mockDaoDriver.attain( pool ).getLastVerified(), "Should notta had a last verified date yet.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertNotNull(mockDaoDriver.attain( pool ).getLastVerified(), "Shoulda updated last verified date.");
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(SuspectBlobPool.class).getCount(), "Shoulda recorded suspect blobs.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(PoolFailure.class).getCount(), "Shoulda generated verification failure.");

        poolPersistence.shutdown();
    }
    
    
    private PoolLockSupport< PoolTask > createLockSupport()
    {
        return new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ),
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }
    
    
    private BasicTestsInvocationHandler getPoolEnvironmentResourceBtih()
    {
        return new BasicTestsInvocationHandler( MockInvocationHandler.forMethod( 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "verifyPool" ),
                new ConstantResponseInvocationHandler( new RpcResponse<>( null ) ),
                null ) );
    }
    
    
    private PoolEnvironmentResource getPoolEnvironmentResource( BasicTestsInvocationHandler btih )
    {
        if ( null == btih )
        {
            btih = getPoolEnvironmentResourceBtih();
        }
        return InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, btih );
    }

    private static DatabaseSupport dbSupport;

    @BeforeAll
    public static void setUpDB() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void setUp() {
        dbSupport.reset();
    }
}
