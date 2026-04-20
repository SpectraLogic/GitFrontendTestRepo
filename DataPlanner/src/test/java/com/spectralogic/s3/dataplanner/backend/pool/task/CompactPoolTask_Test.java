/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.dataplanner.testfrmwrk.PoolTaskBuilder;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.PoolLockSupportImpl;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.testfrmwrk.MockPoolPersistence;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CompactPoolTask_Test 
{
    @Test
    public void testConstructorNullPriorityNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> new PoolTaskBuilder(null)
                        .buildCompactPoolTask( null, UUID.randomUUID()
                ) );
    }
    

    @Test
    public void testConstructorNullPoolIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> new PoolTaskBuilder(null)
                        .buildCompactPoolTask( BlobStoreTaskPriority.values()[ 0 ], null
                ) );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        
        new PoolTaskBuilder(dbSupport.getServiceManager())
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                UUID.randomUUID()
        );
    }
    
    
    @Test
    public void testPrepareForExecutionWhenDesiredPoolLockedDoesNotTransistToPendingExecution()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
    
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "object" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        blob.setChecksum( "foo" )
          .setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( blob, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
    
        final MockPoolPersistence persistence = new MockPoolPersistence( dbSupport, null );
        mockDaoDriver.putBlobOnPool( persistence.getPool()
                                                .getId(), blob.getId() );
        mockDaoDriver.updateBean( persistence.getPool()
                                             .setUsedCapacity( 91 )
                                             .setAvailableCapacity( 4 )
                                             .setReservedCapacity( 5 )
                                             .setTotalCapacity( 100 ), PoolObservable.USED_CAPACITY,
                PoolObservable.AVAILABLE_CAPACITY, PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );
    
        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ], persistence.getPool()
                                                                .getId()
        );
    
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy( PoolTask.class, null );
        
        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
    
        lockSupport.acquireWriteLock( persistence.getPool().getId(), lockHolder, 10, 100 );
        task.prepareForExecutionIfPossible();

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Should notta prepared for execution.");
        assertEquals(null, task.getPoolId(), "Should notta prepared for execution.");
    }
    
    
    @Test
    public void testPrepareForExecutionWhenDesiredPoolAvailableToLockTransistsToPendingExecution()
    {
        
        final MockPoolPersistence persistence = new MockPoolPersistence( dbSupport, null );
    
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        task.prepareForExecutionIfPossible();
        assertEquals(BlobStoreTaskState.PENDING_EXECUTION, task.getState(), "Shoulda prepared for execution.");
        final Object expected = persistence.getPool().getId();
        assertEquals(expected, task.getPoolId(), "Shoulda prepared for execution.");
    }
    

    @Test
    public void testRunWhenPoolBelowCompactionThresholdReturnsCompletedRemovingUnknownBlobsOnPool()
    {
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() ); 
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence persistence = new MockPoolPersistence( dbSupport, null );
        mockDaoDriver.putBlobOnPool( persistence.getPool().getId(), b3.getId() );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setUsedCapacity( 10 ).setAvailableCapacity( 85 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );
        persistence.create( b1, b2, b3 );
        persistence.assertBlobsPersisted( b1, b2, b3 );
        
        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda finished deleting unknown data and been marked ready.");
        persistence.assertBlobsNotPersisted( b1, b2 );
        persistence.assertBlobsPersisted( b3 );

        task.prepareForExecutionIfPossible();
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        persistence.assertBlobsNotPersisted( b1, b2 );
        persistence.assertBlobsPersisted( b3 );

    
        persistence.shutdown();
    }
    
    
    @Test
    public void testRunWhenPoolAboveCompactionThresholdForFirstTwoPhasesPerformsCompactionAndCompletes()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() ); 
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence persistence = new MockPoolPersistence( dbSupport, null );
        mockDaoDriver.putBlobOnPool( persistence.getPool().getId(), b3.getId() );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );
        persistence.create( b1, b2 );
        persistence.assertBlobsPersisted( b1, b2 );
        
        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted();
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted();
        
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setUsedCapacity( 10 ).setAvailableCapacity( 85 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        persistence.assertBlobsPersisted();
        
        persistence.shutdown();
    }
    
    
    @Test
    public void testRunWhenPoolAboveCompactionThresholdForAllPhasesPerformsCompactionAndCompletes()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() ); 
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence persistence = new MockPoolPersistence( dbSupport, null );
        mockDaoDriver.putBlobOnPool( persistence.getPool().getId(), b3.getId() );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );
        persistence.create( b1, b2 );
        persistence.assertBlobsPersisted( b1, b2 );
        
        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted();
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted();
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted();
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
    
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed unknown cleanup.");

        lockSupport.releaseLock( task );
/*//        task.prepareForExecutionIfPossible();

//        TestUtil.invokeAndWaitUnchecked( task );

//        assertEqualsMod(
//                "Shoulda completed.",
//                BlobStoreTaskState.COMPLETED,
//                task.getState() );*/
        persistence.assertBlobsPersisted();

        persistence.shutdown();
    }
    
    
    @Test
    public void testRunWithComplexCompactionsAcrossAllCompactionPhases()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dataPolicy3.getId(), "bucket3" );
        final S3Object o0 = mockDaoDriver.createObject( bucket1.getId(), "o0" );
        final Blob b0 = mockDaoDriver.getBlobFor( o0.getId() ); 
        b0.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b0, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o3 = mockDaoDriver.createObject( bucket3.getId(), "o3" );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        b3.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b3, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o4 = mockDaoDriver.createObject( bucket3.getId(), "o4" );
        final Blob b4 = mockDaoDriver.getBlobFor( o4.getId() );
        b4.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b4, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o5 = mockDaoDriver.createObject( bucket3.getId(), "o5" );
        final Blob b5 = mockDaoDriver.getBlobFor( o5.getId() );
        b5.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b5, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        
        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );
        persistence.create( b0, b1, b2, b3, b4, b5 );
        persistence.assertBlobsPersisted( b0, b1, b2, b3, b4, b5 );
        
        final long millisPerDay = 1000L * 24 * 3600;
        mockDaoDriver.putBlobOnPool( 
                persistence.getPool().getId(), b1.getId(), 
                new Date( System.currentTimeMillis() - millisPerDay * 100 ),
                new Date( System.currentTimeMillis() - millisPerDay * 100 ) );
        mockDaoDriver.putBlobOnPool( 
                persistence.getPool().getId(), b2.getId(), 
                new Date( System.currentTimeMillis() - millisPerDay * 100 ),
                new Date( System.currentTimeMillis() - millisPerDay * 100 ) );
        mockDaoDriver.putBlobOnPool( 
                persistence.getPool().getId(), b3.getId(), 
                new Date( System.currentTimeMillis() - millisPerDay * 10 ),
                new Date( System.currentTimeMillis() - millisPerDay * 10 ) );
        mockDaoDriver.putBlobOnPool( 
                persistence.getPool().getId(), b4.getId(), 
                new Date( System.currentTimeMillis() - millisPerDay * 10 ),
                new Date( System.currentTimeMillis() - millisPerDay * 2 ) );
        mockDaoDriver.putBlobOnPool( 
                persistence.getPool().getId(), b5.getId(), 
                new Date( System.currentTimeMillis() - millisPerDay * 5 ),
                new Date( System.currentTimeMillis() - millisPerDay * 5 ) );
        
        final PoolTask someTask = InterfaceProxyFactory.getProxy( PoolTask.class, null );
        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        lockSupport.acquireReadLock( task.getPoolId(), someTask );
        lockSupport.releaseLock( someTask );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        lockSupport.acquireReadLock( task.getPoolId(), someTask );
        lockSupport.releaseLock( someTask );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted( b1, b2, b3, b4, b5 );
        
        lockSupport.releaseLock( task );
    
        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, PoolLockingException.class,
                () -> lockSupport.acquireReadLock( task.getPoolId(), someTask ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        lockSupport.acquireReadLock( task.getPoolId(), someTask );
        lockSupport.releaseLock( someTask );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted( b1, b2, b4, b5 );
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, PoolLockingException.class,
                () -> lockSupport.acquireReadLock( task.getPoolId(), someTask ) );
        
        TestUtil.invokeAndWaitUnchecked( task );
        
        lockSupport.acquireReadLock( task.getPoolId(), someTask );
        lockSupport.releaseLock( someTask );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another run.");
        persistence.assertBlobsPersisted( b1, b2, b5 );
        
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        TestUtil.assertThrows( null, PoolLockingException.class,
                () -> lockSupport.acquireReadLock( task.getPoolId(), someTask ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed.");
        persistence.assertBlobsPersisted( b1, b2, b5 );
        
        persistence.shutdown();
    }
    
    
    @Test
    public void testFullReclaimOccursWhenPoolNotAssignedToStorageDomainThatHasSinceBeenDeleted()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        persistence.create( b1, b2 );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setAssignedToStorageDomain( true )
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );

        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed after reclaim.");
        persistence.assertBlobsPersisted();
        
        final Pool pool = mockDaoDriver.attain( Pool.class, persistence.getPool().getId() );
        assertEquals(null, pool.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, pool.getBucketId(), "Shoulda reclaimed pool.");
        assertEquals(false, pool.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testFullReclaimOccursWhenPoolNotAssignedToStorageDomain()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        persistence.create( b1, b2 );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );

        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed after reclaim.");
        persistence.assertBlobsPersisted();
        
        final Pool pool = mockDaoDriver.attain( Pool.class, persistence.getPool().getId() );
        assertEquals(null, pool.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, pool.getBucketId(), "Shoulda reclaimed pool.");
        assertEquals(false, pool.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testFullReclaimOccursWhenPoolAssignedToStorageDomainNotBucket()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        persistence.create( b1, b2 );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );

        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed after reclaim.");
        persistence.assertBlobsPersisted();
        
        final Pool pool = mockDaoDriver.attain( Pool.class, persistence.getPool().getId() );
        assertEquals(null, pool.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, pool.getBucketId(), "Shoulda reclaimed pool.");
        assertEquals(false, pool.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testFullReclaimOccursWhenPoolAssignedToStorageDomainAndBucketWithNonSecureIsolation()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        persistence.create( b1, b2 );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                .setBucketId( bucket1.getId() )
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.BUCKET_ID,
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );

        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda completed after reclaim.");
        persistence.assertBlobsPersisted();
        
        final Pool pool = mockDaoDriver.attain( Pool.class, persistence.getPool().getId() );
        assertEquals(null, pool.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, pool.getBucketId(), "Shoulda reclaimed pool.");
        assertEquals(false, pool.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testFullReclaimOccursWhenPoolAssignedToStorageDomainSinceDeletedAndBucket()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        persistence.create( b1, b2 );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setAssignedToStorageDomain( true )
                .setBucketId( bucket1.getId() )
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.BUCKET_ID,
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );

        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Should notta required another pass since reclaim possible.");
        persistence.assertBlobsPersisted();
        
        final Pool pool = mockDaoDriver.attain( Pool.class, persistence.getPool().getId() );
        assertEquals(null, pool.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, pool.getBucketId(), "Shoulda reclaimed pool.");
        assertEquals(false, pool.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testFullReclaimDoesNotOccurWhenPoolAssignedToStorageDomainAndBucketWithSecureIsolation()
    {
        
        final PoolLockSupport< PoolTask > lockSupport = createLockSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy( "dp3" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "poolp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), poolPartition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy1.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.RETIRED, storageDomain.getId() );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createDataPersistenceRule(
                        dataPolicy3.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() )
                             .setMinimumDaysToRetain( 7 ),
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "bucket2" );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() ); 
        b1.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b1, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        b2.setChecksum( "foo" ).setChecksumType( ChecksumType.CRC_32 );
        mockDaoDriver.updateBean( b2, ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );

        final MockPoolPersistence persistence =
                new MockPoolPersistence( dbSupport, poolPartition.getId() );
        persistence.create( b1, b2 );
        mockDaoDriver.updateBean( 
                persistence.getPool()
                .setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                .setBucketId( bucket1.getId() )
                .setUsedCapacity( 91 ).setAvailableCapacity( 4 )
                .setReservedCapacity( 5 ).setTotalCapacity( 100 ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.BUCKET_ID,
                PoolObservable.USED_CAPACITY, PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.RESERVED_CAPACITY, PoolObservable.TOTAL_CAPACITY );

        final CompactPoolTask task = new PoolTaskBuilder(dbSupport.getServiceManager())
                .withLockSupport(lockSupport)
                .buildCompactPoolTask(
                BlobStoreTaskPriority.values()[ 0 ],
                persistence.getPool().getId()
        );
        lockSupport.releaseLock( task );
        task.prepareForExecutionIfPossible();
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.READY, task.getState(), "Shoulda required another pass since reclaim not possible.");
        persistence.assertBlobsPersisted();
        
        final Pool pool = mockDaoDriver.attain( Pool.class, persistence.getPool().getId() );
        final Object expected1 = sdm.getId();
        assertEquals(expected1, pool.getStorageDomainMemberId(), "Should notta reclaimed pool.");
        final Object expected = bucket1.getId();
        assertEquals(expected, pool.getBucketId(), "Should notta reclaimed pool.");
        assertEquals(true, pool.isAssignedToStorageDomain(), "Should notta reclaimed pool.");
    }
    
    private PoolLockSupport< PoolTask > createLockSupport()
    {
        return new PoolLockSupportImpl<>(
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }
    
    
    private PoolEnvironmentResource getPoolEnvironmentResource()
    {
        return InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, null );
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
