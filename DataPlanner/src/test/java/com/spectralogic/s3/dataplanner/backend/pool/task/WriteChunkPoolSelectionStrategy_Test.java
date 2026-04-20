/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainFailureService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.frmwrk.CanAllocatePersistenceTargetSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class WriteChunkPoolSelectionStrategy_Test 
{
    @Test
    public void testCapacityWriteOptimizationPoolSelectedWhichIsAlreadyAssignedToBucketWhenPossible()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition partition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), partition.getId() ); 
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ), 
                sd.setWriteOptimization( WriteOptimization.CAPACITY ) );
        
        final Pool pool1 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 500L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Pool pool2 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 500L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() )
                .setState( PoolState.NORMAL );
        final Pool pool3 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 500L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.LOST )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( pool1 );
        dbSupport.getDataManager().createBean( pool2 );
        dbSupport.getDataManager().createBean( pool3 );
        
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final Object expected4 = pool1.getId();
        assertEquals(expected4, strategy.selectPool( blob.getLength(), sd.getId(), bucket.getId(), new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        assertEquals(100L * 1024 * 1024 * 1024,  blob.getLength(), "Shoulda gotten correct current size of bucket.");
        final Object expected3 = pool1.getId();
        assertEquals(expected3, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        final Object expected2 = pool1.getId();
        assertEquals(expected2, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        final Object expected1 = pool1.getId();
        assertEquals(expected1, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        assertEquals(null, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        CollectionFactory.toSet( pool1.getId() ) ), "Shoulda throttle-prevented selection of pool not in exclusion set.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");

        final S3Object object2 = mockDaoDriver.createObject( bucket.getId(), "o2", -1 );
        final List< Blob > extraBlobs =
                mockDaoDriver.createBlobs( object2.getId(), 5, 100L * 1024 * 1024 * 1024 );
        final Set<JobEntry> entries = mockDaoDriver.createJobEntries( job.getId(), extraBlobs );
        mockDaoDriver.createPersistenceTargetsForChunks(entries);
        assertEquals(null, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        CollectionFactory.toSet( pool1.getId() ) ), "Shoulda cached write optimization data.");
        CanAllocatePersistenceTargetSupport.clearCachedBucketWriteOptimizationData();
        final Object expected = pool2.getId();
        assertEquals(expected, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        CollectionFactory.toSet( pool1.getId() ) ), "Shoulda selected pool not in exclusion set.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");
    }


    @Test
    public void testCapacityWriteOptimizationThrottlingCannotResultInStorageDomainStallFailure()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition partition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd.setWriteOptimization( WriteOptimization.CAPACITY ) );

        final Pool pool1 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 500L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        dbSupport.getDataManager().createBean( pool1 );

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());
        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final Object expected3 = pool1.getId();
        assertEquals(expected3, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        assertEquals(100L * 1024 * 1024 * 1024,  blob.getLength(), "Shoulda gotten correct current size of bucket.");
        final Object expected2 = pool1.getId();
        assertEquals(expected2, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        final Object expected1 = pool1.getId();
        assertEquals(expected1, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        final Object expected = pool1.getId();
        assertEquals(expected, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool already assigned to bucket.");
        assertEquals(null, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        CollectionFactory.toSet( pool1.getId() ) ), "Shoulda throttle-prevented selection of pool not in exclusion set.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");
    }


    @Test
    public void testCapacityWriteOptimizationOnlyConsidersIndividualMediaThatIsActuallyUsable()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition partition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd.setWriteOptimization( WriteOptimization.CAPACITY ) );

        final Pool pool1 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 99L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Pool pool2 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 100L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setQuiesced( Quiesced.YES )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Pool pool3 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 100L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( pool1 );
        dbSupport.getDataManager().createBean( pool2 );
        dbSupport.getDataManager().createBean( pool3 );

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", -1 );
        final List< Blob > blobs = mockDaoDriver.createBlobs( object.getId(), 100, 1L * 1024 * 1024 * 1024 );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final Set<JobEntry> entries = mockDaoDriver.createJobEntries( job.getId(), blobs );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunks(entries);
        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final Object expected = pool3.getId();
        assertEquals(expected, strategy.selectPool(blobs.stream().mapToLong(Blob::getLength).sum(), sd.getId(), bucket.getId(), new HashSet<>() ), "Shoulda selected pool not assigned to bucket since no single unquiesced pool will fit the chunk.");
    }


    @Test
    public void testPerformanceWriteOptimizationPoolSelectedWhichIsNotAlreadyAssignedToBucketWhenPossible()
    {
        
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition partition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), partition.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final Pool pool1 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 500L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm.getId() );
        final Pool pool2 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 400L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() )
                .setState( PoolState.NORMAL );
        final Pool pool3 = BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setAvailableCapacity( 500L * 1024 * 1024 * 1024 )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.LOST )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( pool1 );
        dbSupport.getDataManager().createBean( pool2 );
        dbSupport.getDataManager().createBean( pool3 );

        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "o1", 100L * 1024 * 1024 * 1024 );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final Job job = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final Object expected4 = pool1.getId();
        assertEquals(expected4, strategy.selectPool( blob.getLength(), sd.getId(), bucket.getId(), new HashSet<UUID>() ), "Shoulda selected pool with most space available.");
        assertEquals(100L * 1024 * 1024 * 1024,  blob.getLength(), "Shoulda gotten correct current size of bucket.");
        final Object expected3 = pool2.getId();
        assertEquals(expected3, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool not already assigned to bucket.");
        final Object expected2 = pool1.getId();
        assertEquals(expected2, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool with more space available");
        final Object expected1 = pool1.getId();
        assertEquals(expected1, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        new HashSet<UUID>() ), "Shoulda selected pool with more space available");
        final Object expected = pool2.getId();
        assertEquals(expected, strategy.selectPool(
                        blob.getLength(),
                        sd.getId(),
                        bucket.getId(),
                        CollectionFactory.toSet( pool1.getId() ) ), "Should notta throttle-prevented selection of pool not in exclusion set.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated any failures.");
    }
    
    
    @Test
    public void testSelectPoolCreatesFailureWhenNoFreePoolsRemaining()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final PoolPartition partition =
                mockDaoDriver.createPoolPartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd.getId(), partition.getId() ); 
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ), 
                sd.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final UUID jobId = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT ).getId();
        final JobEntry chunk = mockDaoDriver.createJobEntry( jobId );
        final Set<LocalBlobDestination> pts = mockDaoDriver.createPersistenceTargetsForChunk(chunk.getId());

        final WriteChunkPoolSelectionStrategy tapeSelector =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final UUID selectedPool = tapeSelector.selectPool(
                12000,
                sd.getId(),
                bucket.getId(),
                new HashSet< UUID >() );
        assertNull(
                selectedPool,
                "Should notta selected a pool since none of them had enough space." );
        
        final StorageDomainFailureService failureService = dbSupport
                .getServiceManager()
                .getService( StorageDomainFailureService.class );
        final StorageDomainFailure mainFailure =
                failureService.retrieve( Require.nothing() );
        assertNotNull(mainFailure, "Shoulda created a partition failure for the main partition.");
        assertNotNull(mainFailure.getErrorMessage(), "Shoulda had an error message.");
        assertEquals(StorageDomainFailureType.WRITES_STALLED_DUE_TO_NO_FREE_MEDIA_REMAINING, mainFailure.getType(), "Shoulda had the writes stalled failure type.");
    }


    @Test
    public void testSelectPoolIgnoresUnacceptablePools()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final Pool pool1 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );

        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "2" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd1.getId(), dp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), dp2.getId() );

        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );

        // Set PERFORMANCE optimization so CanAllocatePersistenceTargetSupport always allows allocation
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd1.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd2.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );

        // Insufficient capacity: pool has 10000 (default), but test with capacity set to 998
        mockDaoDriver.updateBean(
                pool1.setAvailableCapacity( 998 ), PoolObservable.AVAILABLE_CAPACITY );
        assertNull( strategy.selectPool( 1000, sd1.getId(), bucket.getId(), new HashSet<>() ),
                "Should not select pool with insufficient capacity." );

        // Sufficient capacity but pool in wrong partition (dp2, not sd1's dp1)
        mockDaoDriver.updateBean(
                pool1.setAvailableCapacity( 1000 ), PoolObservable.AVAILABLE_CAPACITY );
        mockDaoDriver.updateBean(
                pool1.setPartitionId( dp2.getId() ), Pool.PARTITION_ID );
        assertNull( strategy.selectPool( 1000, sd1.getId(), bucket.getId(), new HashSet<>() ),
                "Should not select pool in wrong partition." );

        // Pool back in correct partition — found and allocated on this call
        mockDaoDriver.updateBean(
                pool1.setPartitionId( dp1.getId() ), Pool.PARTITION_ID );
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd1.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool in correct partition with sufficient capacity." );

        // Pool is now allocated to sd1. Test LOST state.
        mockDaoDriver.updateBean(
                pool1.setState( PoolState.LOST ), Pool.STATE );
        assertNull( strategy.selectPool( 1000, sd1.getId(), bucket.getId(), new HashSet<>() ),
                "Should not select pool in LOST state." );

        // Back to NORMAL
        mockDaoDriver.updateBean(
                pool1.setState( PoolState.NORMAL ), Pool.STATE );
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd1.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool in NORMAL state." );

        // sd2 has dp2 but pool1 is allocated to sd1 — not found for sd2
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "Should not select pool allocated to different storage domain." );

        // Pool in unavailable set
        assertNull( strategy.selectPool( 1000, sd1.getId(), bucket.getId(),
                CollectionFactory.toSet( pool1.getId() ) ),
                "Should not select pool in unavailable set." );
    }


    @Test
    public void testSelectPoolGeneratesOutOfMediaFailuresUntilMediaAvailable()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob1 = mockDaoDriver.getBlobFor( o1.getId() );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "dp2" );

        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "4" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), dp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd3.getId(), dp1.getId(), WritePreferenceLevel.HIGH );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd4.getId(), dp2.getId() );

        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY,
                sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                sd4.getId() );

        mockDaoDriver.createBucket( null, "otherbucket" );

        // PERFORMANCE optimization so allocation is always allowed
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd2.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd3.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd4.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final StorageDomainFailureService failureService =
                dbSupport.getServiceManager().getService( StorageDomainFailureService.class );

        // === Section 1: No pools available ===
        assertEquals( 0, failureService.getCount(), "No failures initially." );

        // Each storage domain that can't find a pool generates a failure via WritesStalledSupport
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "No pools available for sd2." );
        assertNull( strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "No pools available for sd3." );
        assertNull( strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "No pools available for sd4." );
        assertTrue( failureService.getCount() > 0,
                "Should have recorded out-of-media failures." );

        // === Section 2: Pools become available ===
        final Pool pool1 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool1.setAvailableCapacity( 20000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool2 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool2.setAvailableCapacity( 10000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool3 = mockDaoDriver.createPool( dp2.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool3.setAvailableCapacity( 20000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool4 = mockDaoDriver.createPool( dp2.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool4.setAvailableCapacity( 10000 ), PoolObservable.AVAILABLE_CAPACITY );

        // sd2 has dp1: pool1 (20000, highest capacity) selected and allocated
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool1 (highest capacity in dp1) for sd2." );

        // sd3 has dp1: pool1 now allocated to sd2, pool2 (next available) selected
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool2 (remaining unallocated in dp1) for sd3." );

        // sd4 has dp2: pool3 (20000, highest capacity) selected and allocated
        assertEquals( pool3.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool3 (highest capacity in dp2) for sd4." );

        // Failures should be cleared for storage domains that successfully allocated
        assertEquals( 0, failureService.getCount(),
                "Failures should be cleared now that media was allocated." );

        // Pool in unavailable set — falls back to allocated pool
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd2.getId(), bucket.getId(),
                        CollectionFactory.toSet( pool2.getId() ) ),
                "Should find allocated pool1 for sd2 when pool2 excluded." );

        // sd4 with pool3 unavailable — should find pool4 (unallocated) or pool3 excluded
        // pool3 is allocated to sd4, pool4 is unallocated in dp2
        assertEquals( pool4.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(),
                        CollectionFactory.toSet( pool3.getId() ) ),
                "Should select pool4 when pool3 excluded for sd4." );
    }


    @Test
    public void testSelectPoolReturnsCorrectlyWhenBucketIsolated()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final PoolPartition dp3 = mockDaoDriver.createPoolPartition( null, "dp3" );

        // Create pools with distinct capacities for deterministic DESC sorting
        final Pool pool1 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool1.setAvailableCapacity( 50000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool2 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool2.setAvailableCapacity( 40000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool3 = mockDaoDriver.createPool( dp2.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool3.setAvailableCapacity( 45000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool4 = mockDaoDriver.createPool( dp2.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool4.setAvailableCapacity( 35000 ), PoolObservable.AVAILABLE_CAPACITY );
        mockDaoDriver.createPool( dp3.getId(), PoolState.NORMAL ); // pool5 in dp3

        // sd2: dp1 pool member + dp3 pool member (NEVER_SELECT)
        // sd3: dp1 pool member (HIGH write pref)
        // sd4: dp2 pool member
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "4" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), dp1.getId() );
        final StorageDomainMember sdm3 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd3.getId(), dp1.getId(), WritePreferenceLevel.HIGH );
        final StorageDomainMember sdm4 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd4.getId(), dp2.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), dp3.getId(), WritePreferenceLevel.NEVER_SELECT );

        // BUCKET_ISOLATED isolation (getIsolatedBucketId returns bucket.getId())
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT, sd4.getId() );

        final Bucket bucket2 = mockDaoDriver.createBucket( null, "otherbucket" );

        // PERFORMANCE optimization so allocation is always allowed
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd2.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd3.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd4.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );

        // === Section 1: Unallocated pool allocation cascade ===
        // Strategy allocates with bucketId=bucket.getId() (BUCKET_ISOLATED)
        // sd2 (dp1): pool1(50000) found unallocated, allocated to sd2
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool1 (highest capacity in dp1) for sd2." );

        // sd3 (dp1): pool1 now allocated to sd2, pool2(40000) next, allocated to sd3
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool2 (remaining unallocated in dp1) for sd3." );

        // sd4 (dp2): pool3(45000) found, allocated to sd4
        assertEquals( pool3.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool3 (highest capacity in dp2) for sd4." );

        // pool5 in dp3 has NEVER_SELECT write preference — excluded
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(),
                CollectionFactory.toSet( pool1.getId() ) ),
                "NEVER_SELECT member excludes dp3 pools for sd2." );

        // Allocated pool with correct bucketId=bucket.getId() found
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Should find allocated pool2 for sd3." );

        // sd4 with pool3 excluded: pool4(35000) unallocated, allocated with bucketId=bucket.getId()
        assertEquals( pool4.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(),
                        CollectionFactory.toSet( pool3.getId() ) ),
                "Should select pool4 when pool3 excluded for sd4." );

        // === Section 2: Reassign pool1 to sd4, pool4 to sd4 with bucket2 (wrong bucket) ===
        poolService.update(
                pool1.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        poolService.update(
                pool4.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4.getId() )
                        .setBucketId( bucket2.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.BUCKET_ID );

        // sd2: pool1 moved to sd4, no unallocated dp1, pool2 in sd3 — nothing for sd2
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 moved to sd4, no pools left for sd2." );

        // sd3: pool2 allocated with bucketId=bucket.getId() ✓
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Pool2 still allocated to sd3 with correct bucket." );

        // sd4: pool1(bucketId=bucket.getId() ✓, 50000), pool3(bucketId=bucket.getId() ✓, 45000),
        //      pool4(bucketId=bucket2 ✗) → pool1
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 has correct bucketId, highest cap for sd4." );

        // === Section 3: Reassign pool1 to sd3, pool4 bucketId back to bucket ===
        poolService.update(
                pool1.setStorageDomainMemberId( sdm3.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        poolService.update(
                pool4.setBucketId( bucket.getId() ),
                PersistenceTarget.BUCKET_ID );

        // sd3: pool1(sdm3, bucketId=bucket.getId() ✓, 50000), pool2(sdm3, bucketId=bucket.getId() ✓, 40000)
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 in sd3 with correct bucket, highest cap." );

        // sd4: pool3(bucketId=bucket.getId() ✓, 45000), pool4(bucketId=bucket.getId() ✓, 35000) → pool3
        assertEquals( pool3.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Pool3 highest cap with correct bucket for sd4." );

        // sd2: still nothing
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "No pools for sd2." );

        // === Section 4: Set pool3 to bucket2 (wrong), test bucket isolation rejection ===
        poolService.update(
                pool2.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm3.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        poolService.update(
                pool3.setBucketId( bucket2.getId() ),
                PersistenceTarget.BUCKET_ID );

        // sd3: pool1(bucket.getId() ✓), pool2(bucket.getId() ✓) → pool1
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 still best for sd3." );

        // sd4: pool3(bucket2 ✗), pool4(bucket.getId() ✓, 35000) → pool4
        assertEquals( pool4.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Pool3 has wrong bucket, pool4 found for sd4." );

        // Set pool4 bucketId to null — rejected under BUCKET_ISOLATED (requires bucket.getId())
        poolService.update(
                pool4.setBucketId( null ),
                PersistenceTarget.BUCKET_ID );

        // sd4: pool3(bucket2 ✗), pool4(null ✗ for BUCKET_ISOLATED) → null
        assertNull( strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Null bucketId rejected under BUCKET_ISOLATED (requires bucket.getId())." );

        // sd2: no pools
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "No pools for sd2." );
    }


    @Test
    public void testSelectPoolReturnsCorrectlyWhenBucketNotIsolated()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );

        final PoolPartition dp1 = mockDaoDriver.createPoolPartition( null, "dp1" );
        final PoolPartition dp2 = mockDaoDriver.createPoolPartition( null, "dp2" );
        final PoolPartition dp3 = mockDaoDriver.createPoolPartition( null, "dp3" );

        // Create pools with distinct capacities for deterministic DESC sorting
        final Pool pool1 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool1.setAvailableCapacity( 50000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool2 = mockDaoDriver.createPool( dp1.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool2.setAvailableCapacity( 40000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool3 = mockDaoDriver.createPool( dp2.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool3.setAvailableCapacity( 45000 ), PoolObservable.AVAILABLE_CAPACITY );
        final Pool pool4 = mockDaoDriver.createPool( dp2.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( pool4.setAvailableCapacity( 35000 ), PoolObservable.AVAILABLE_CAPACITY );
        mockDaoDriver.createPool( dp3.getId(), PoolState.NORMAL ); // pool5 in dp3

        // sd2: dp1 pool member + dp3 pool member (NEVER_SELECT)
        // sd3: dp1 pool member (HIGH write pref)
        // sd4: dp2 pool member
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "4" );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd2.getId(), dp1.getId() );
        final StorageDomainMember sdm3 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd3.getId(), dp1.getId(), WritePreferenceLevel.HIGH );
        final StorageDomainMember sdm4 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd4.getId(), dp2.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), dp3.getId(), WritePreferenceLevel.NEVER_SELECT );

        // STANDARD isolation persistence rules (getIsolatedBucketId returns null)
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT, sd4.getId() );

        final Bucket bucket2 = mockDaoDriver.createBucket( null, "otherbucket" );

        // PERFORMANCE optimization so allocation is always allowed
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd2.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd3.setWriteOptimization( WriteOptimization.PERFORMANCE ) );
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( StorageDomain.WRITE_OPTIMIZATION ),
                sd4.setWriteOptimization( WriteOptimization.PERFORMANCE ) );

        final WriteChunkPoolSelectionStrategy strategy =
                new WriteChunkPoolSelectionStrategy( dbSupport.getServiceManager() );
        final PoolService poolService = dbSupport.getServiceManager().getService( PoolService.class );

        // === Section 1: Unallocated pool allocation cascade ===
        // sd2 (dp1): pool1(50000, highest cap) found unallocated, allocated to sd2
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool1 (highest capacity in dp1) for sd2." );

        // sd3 (dp1): pool1 now allocated to sd2, pool2(40000) next unallocated, allocated to sd3
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool2 (remaining unallocated in dp1) for sd3." );

        // sd4 (dp2): pool3(45000, highest cap) found unallocated, allocated to sd4
        assertEquals( pool3.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Should select pool3 (highest capacity in dp2) for sd4." );

        // pool5 in dp3 has NEVER_SELECT write preference for sd2 — excluded
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(),
                CollectionFactory.toSet( pool1.getId() ) ),
                "NEVER_SELECT member excludes dp3 pools for sd2." );

        // Allocated pool found via allocated path
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Should find allocated pool2 for sd3." );

        // sd4 with pool3 excluded: pool4(35000) unallocated in dp2, allocated to sd4
        assertEquals( pool4.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(),
                        CollectionFactory.toSet( pool3.getId() ) ),
                "Should select pool4 when pool3 excluded for sd4." );

        // === Section 2: Reassign pool1 to sd4, pool4 to sd4 with bucket2 ===
        poolService.update(
                pool1.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        poolService.update(
                pool4.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm4.getId() )
                        .setBucketId( bucket2.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                PersistenceTarget.BUCKET_ID );

        // sd2: pool1 moved to sd4, no unallocated in dp1, pool2 in sd3 — nothing for sd2
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 moved to sd4, no pools left for sd2." );

        // sd3: pool2 still allocated to sd3 with null bucketId (STANDARD)
        assertEquals( pool2.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Pool2 still allocated to sd3." );

        // sd4: pool1(50000, null bucket ✓), pool3(45000, null bucket ✓),
        //      pool4(35000, bucket2 ≠ null ✗ for STANDARD isolation)
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 (highest cap, null bucket) found for sd4." );

        // === Section 3: Reassign pool1 to sd3, pool4 to sd4 with bucket (non-null) ===
        poolService.update(
                pool1.setStorageDomainMemberId( sdm3.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        poolService.update(
                pool4.setBucketId( bucket.getId() ),
                PersistenceTarget.BUCKET_ID );

        // sd3: pool1(50000, sdm3, null bucket) and pool2(40000, sdm3, null bucket), DESC → pool1
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 now in sd3 with highest capacity." );

        // sd4: pool3(sdm4, null bucket ✓), pool4(sdm4, bucket.getId() ≠ null ✗) → pool3
        assertEquals( pool3.getId(),
                strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "Pool4 has non-null bucketId, rejected by STANDARD isolation." );

        // sd2: still nothing
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "No pools for sd2 after reassignments." );

        // === Section 4: All pools exhausted for some domains ===
        poolService.update(
                pool2.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm3.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        poolService.update(
                pool3.setBucketId( bucket2.getId() ),
                PersistenceTarget.BUCKET_ID );

        // sd3: pool1(50000, null bucket) and pool2(40000, null bucket) → pool1
        assertEquals( pool1.getId(),
                strategy.selectPool( 1000, sd3.getId(), bucket.getId(), new HashSet<>() ),
                "Pool1 still highest cap in sd3." );

        // sd4: pool3(bucket2 ≠ null ✗), pool4(bucket ≠ null ✗) → null
        assertNull( strategy.selectPool( 1000, sd4.getId(), bucket.getId(), new HashSet<>() ),
                "All sd4 pools have non-null bucketId, rejected by STANDARD." );

        // sd2: no pools allocated to sd2, no unallocated in dp1
        assertNull( strategy.selectPool( 1000, sd2.getId(), bucket.getId(), new HashSet<>() ),
                "No pools for sd2." );
    }


    private static DatabaseSupport dbSupport;

    @BeforeAll
     static void setUpDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void setUp() {
        dbSupport.reset();
    }
}
