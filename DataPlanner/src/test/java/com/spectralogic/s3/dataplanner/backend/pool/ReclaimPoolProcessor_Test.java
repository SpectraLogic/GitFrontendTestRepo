/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.lang.reflect.InvocationHandler;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class ReclaimPoolProcessor_Test 
{
    @Test
    public void testConstructorNullNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Override
        public void test()
            {
                new ReclaimPoolProcessor( null, getLockSupport() );
            }
        } );
    }
    
    
    @Test
    public void testPoolAllocatedToStorageDomainAndNotBucketWithoutDataOnItReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( sdm.getId() )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor =
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();
        
        final Pool p2 = mockDaoDriver.attain( p );
        assertEquals(null, p2.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, p2.getBucketId(), "Shoulda reclaimed pool.");
        assertFalse(p2.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testPoolAllocatedToStorageDomainAndNotBucketWithDataOnItNotReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.putBlobOnPool( p.getId(), blob.getId() );
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( sdm.getId() )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( null ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor =
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();
        
        final Pool p2 = mockDaoDriver.attain( p );
        final Object expected1 = p.getStorageDomainMemberId();
        assertEquals(expected1, p2.getStorageDomainMemberId(), "Should notta reclaimed pool.");
        final Object expected = p.getBucketId();
        assertEquals(expected, p2.getBucketId(), "Should notta reclaimed pool.");
        assertTrue(p2.isAssignedToStorageDomain(), "Should notta reclaimed pool.");
    }
    
    
    @Test
    public void testPoolAllocatedToStorageDomainAndBucketNonSecurelyWithoutDataOnItReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( sdm.getId() )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor = 
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();
        
        final Pool p2 = mockDaoDriver.attain( p );
        assertEquals(null, p2.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, p2.getBucketId(), "Shoulda reclaimed pool.");
        assertFalse(p2.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testPoolAllocatedToStorageDomainAndBucketNonSecurelyWithDataOnItNotReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.putBlobOnPool( p.getId(), blob.getId() );
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( sdm.getId() )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor = 
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();
        
        final Pool p2 = mockDaoDriver.attain( p );
        final Object expected1 = p.getStorageDomainMemberId();
        assertEquals(expected1, p2.getStorageDomainMemberId(), "Should notta reclaimed pool.");
        final Object expected = p.getBucketId();
        assertEquals(expected, p2.getBucketId(), "Should notta reclaimed pool.");
        assertTrue(p2.isAssignedToStorageDomain(), "Should notta reclaimed pool.");
    }
    
    
    @Test
    public void testPoolAllocatedToStorageDomainAndBucketSecurelyWithoutDataOnItNotReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( sdm.getId() )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor = 
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();
        
        Pool p2 = mockDaoDriver.attain( p );
        final Object expected3 = p.getStorageDomainMemberId();
        assertEquals(expected3, p2.getStorageDomainMemberId(), "Should notta reclaimed pool.");
        final Object expected2 = p.getBucketId();
        assertEquals(expected2, p2.getBucketId(), "Should notta reclaimed pool.");
        assertTrue(p2.isAssignedToStorageDomain(), "Should notta reclaimed pool.");
        processor.run();
        
        p2 = mockDaoDriver.attain( p );
        final Object expected1 = p.getStorageDomainMemberId();
        assertEquals(expected1, p2.getStorageDomainMemberId(), "Should notta reclaimed pool.");
        final Object expected = p.getBucketId();
        assertEquals(expected, p2.getBucketId(), "Should notta reclaimed pool.");
        assertTrue(p2.isAssignedToStorageDomain(), "Should notta reclaimed pool.");
    }
    
    
    @Test
    public void testPoolAllocatedToStorageDomainAndBucketSecurelyWithDataOnItNotReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        final StorageDomainMember sdm = mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.putBlobOnPool( p.getId(), blob.getId() );
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( sdm.getId() )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor =
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();
        
        final Pool p2 = mockDaoDriver.attain( p );
        final Object expected1 = p.getStorageDomainMemberId();
        assertEquals(expected1, p2.getStorageDomainMemberId(), "Should notta reclaimed pool.");
        final Object expected = p.getBucketId();
        assertEquals(expected, p2.getBucketId(), "Should notta reclaimed pool.");
        assertTrue(p2.isAssignedToStorageDomain(), "Should notta reclaimed pool.");
    }
    
    
    @Test
    public void testPoolWithoutDataOnItNotMappedToByStorageDomainReclaimed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( null )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final ReclaimPoolProcessor processor =
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), getLockSupport() );
        processor.run();

        final Pool p2 = mockDaoDriver.attain( p );
        assertEquals(null, p2.getStorageDomainMemberId(), "Shoulda reclaimed pool.");
        assertEquals(null, p2.getBucketId(), "Shoulda reclaimed pool.");
        assertFalse(p2.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");
    }
    
    
    @Test
    public void testFailureToAcquireLockOnPoolPreventsItsReclaim()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( null, "pp" );
        mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), partition.getId() );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( "dp" );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dp.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o = mockDaoDriver.createObject( bucket.getId(), "o1" );
        mockDaoDriver.getBlobFor( o.getId() );
        final Pool p = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( 
                p.setStorageDomainMemberId( null )
                 .setAssignedToStorageDomain( true )
                 .setBucketId( bucket.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.BUCKET_ID );
        
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy( PoolTask.class, null );
        final PoolLockSupport< PoolTask > lockSupport = new PoolLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ),
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
        lockSupport.acquireReadLock( p.getId(), lockHolder );
        final ReclaimPoolProcessor processor =
                new ReclaimPoolProcessor( dbSupport.getServiceManager(), lockSupport );
        processor.run();
        
        mockDaoDriver.attainAndUpdate( p );
        assertTrue(p.isAssignedToStorageDomain(), "Should notta reclaimed pool.");

        lockSupport.releaseLock( lockHolder );
        processor.run();

        mockDaoDriver.attainAndUpdate( p );
        assertFalse(p.isAssignedToStorageDomain(), "Shoulda reclaimed pool.");

        assertEquals(0,  lockSupport.getPoolsUnavailableForWriteLock().size(), "Shoulda returned no pools locked.");
    }
    
    
    private PoolLockSupport< PoolTask > getLockSupport()
    {
        return getLockSupport( null );
    }
    
    
    @SuppressWarnings( "unchecked" )
    private PoolLockSupport< PoolTask > getLockSupport( final InvocationHandler ih )
    {
        return InterfaceProxyFactory.getProxy( PoolLockSupport.class, ih );
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
