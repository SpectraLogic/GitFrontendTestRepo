/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.pool.PoolService.PoolAccessType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class PoolServiceImpl_Test 
{
    @Test
    public void testGetAvailableSpacesForBucketReturnsAvailableSpace()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy1 = mockDaoDriver.createDataPolicy( "dp1" );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy( "dp2" );
        final Bucket b1 = mockDaoDriver.createBucket( null, dataPolicy1.getId(), "b1" );
        final Bucket b2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b2" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        mockDaoDriver.createStorageDomain( "sd4" );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "partition" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd1.getId(), partition.getId() );
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), partition.getId() );
        final StorageDomainMember sdm3 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd3.getId(), partition.getId() );

        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy1.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy1.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy1.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                sd3.getId() );
        
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD, 
                dataPolicy2.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD, 
                dataPolicy2.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule( 
                DataIsolationLevel.STANDARD, 
                dataPolicy2.getId(), 
                DataPersistenceRuleType.TEMPORARY,
                sd3.getId() );
        
        // Pools usable to b1 in sd1 (degraded health does not preclude usage of the pool)
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableCapacity( 0 ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableCapacity( 1 ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.DEGRADED )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableCapacity( 2 ) );
        
        // Pools usable to b1 in sd2
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm2.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableCapacity( 10 ) );
        
        // Pools usable to b2 in sd1
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAvailableCapacity( 100 ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAvailableCapacity( 200 ) );
        
        // Pools usable to b2 in sd3
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.NORMAL )
                .setStorageDomainMemberId( sdm3.getId() )
                .setAvailableCapacity( 1000 ) );
        
        // Unusable tapes since not normal state
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.LOST )
                .setStorageDomainMemberId( sdm3.getId() )
                .setAvailableCapacity( 9 ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Pool.class )
                .setGuid( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.OK )
                .setName( UUID.randomUUID().toString() )
                .setType( PoolType.values()[ 0 ] )
                .setState( PoolState.LOST )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAvailableCapacity( 99999 ) );
        
        asertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 0, 1, 2 },
                dbSupport.getServiceManager().getService( PoolService.class ).getAvailableSpacesForBucket(
                        b1.getId(), sd1.getId() ) );
        asertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 10 },
                dbSupport.getServiceManager().getService( PoolService.class ).getAvailableSpacesForBucket(
                        b1.getId(), sd2.getId() ) );
        asertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] {},
                dbSupport.getServiceManager().getService( PoolService.class ).getAvailableSpacesForBucket(
                        b1.getId(), sd3.getId() ) );
        
        asertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 100, 200 },
                dbSupport.getServiceManager().getService( PoolService.class ).getAvailableSpacesForBucket(
                        b2.getId(), sd1.getId() ) );
        asertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] {},
                dbSupport.getServiceManager().getService( PoolService.class ).getAvailableSpacesForBucket(
                        b2.getId(), sd2.getId() ) );
        asertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 1000 },
                dbSupport.getServiceManager().getService( PoolService.class ).getAvailableSpacesForBucket(
                        b2.getId(), sd3.getId() ) );
    }
    
    
    private void asertArraysEqual( final String message, final long [] expected, final long [] actual )
    {
        assertEquals(expected.length,  actual.length, message);
        for ( int i = 0; i < actual.length; ++i )
        {
            assertEquals(expected[ i ],  actual[i], "At index " + i + ", " + message);
        }
    }
    
    
    @Test
    public void testUpdateDatesForVerifiedDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Pool pool1 = mockDaoDriver.createPool();
        Pool pool2 = mockDaoDriver.createPool();
        
        final Date lastVerifiedDate = new Date();
        mockDaoDriver.updateBean(
                pool1.setLastVerified( lastVerifiedDate ), PersistenceTarget.LAST_VERIFIED );
        pool1 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool1.getId() );
        pool2 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool2.getId() );

        assertEquals(null, service.attain( pool1.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool1.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(lastVerifiedDate, service.attain( pool1.getId() ).getLastVerified(), "Shoulda had date set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastVerified(), "Should notta had dates set initially.");

        TestUtil.sleep( 2 );
        service.updateDates( pool1.getId(), PoolAccessType.VERIFIED );
        pool1 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool1.getId() );
        pool2 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool2.getId() );

        assertNotNull(service.attain( pool1.getId() ).getLastAccessed(), "Shoulda updated last verified/accessed date on pool1 only.");
        assertEquals(null, service.attain( pool1.getId() ).getLastModified(), "Shoulda updated last verified date on pool1 only.");
        assertFalse(lastVerifiedDate.equals( pool1.getLastVerified() ), "Shoulda updated last verified date on pool1 only.");
        assertNotNull(pool1.getLastVerified(), "Shoulda updated last verified date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastAccessed(), "Shoulda updated last verified date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastModified(), "Shoulda updated last verified date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastVerified(), "Shoulda updated last verified date on pool1 only.");
    }
    
    
    @Test
    public void testUpdateDatesForAccessedDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Pool pool1 = mockDaoDriver.createPool();
        Pool pool2 = mockDaoDriver.createPool();
        
        final Date lastVerifiedDate = new Date();
        mockDaoDriver.updateBean(
                pool1.setLastVerified( lastVerifiedDate ), PersistenceTarget.LAST_VERIFIED );
        pool1 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool1.getId() );
        pool2 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool2.getId() );

        assertEquals(null, service.attain( pool1.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool1.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(lastVerifiedDate, service.attain( pool1.getId() ).getLastVerified(), "Shoulda had date set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastVerified(), "Should notta had dates set initially.");

        service.updateDates( pool1.getId(), PoolAccessType.ACCESSED );
        pool1 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool1.getId() );
        pool2 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool2.getId() );

        assertNotNull(service.attain( pool1.getId() ).getLastAccessed(), "Shoulda updated last accessed date on pool1 only.");
        assertEquals(null, service.attain( pool1.getId() ).getLastModified(), "Shoulda updated last accessed date on pool1 only.");
        assertEquals(lastVerifiedDate, pool1.getLastVerified(), "Shoulda updated last accessed date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastAccessed(), "Shoulda updated last accessed date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastModified(), "Shoulda updated last accessed date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastVerified(), "Shoulda updated last accessed date on pool1 only.");
    }
    
    
    @Test
    public void testUpdateDatesForModifiedDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Pool pool1 = mockDaoDriver.createPool();
        Pool pool2 = mockDaoDriver.createPool();

        final Date lastVerifiedDate = new Date();
        mockDaoDriver.updateBean(
                pool1.setLastVerified( lastVerifiedDate ), PersistenceTarget.LAST_VERIFIED );
        pool1 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool1.getId() );
        pool2 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool2.getId() );

        assertEquals(null, service.attain( pool1.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool1.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(lastVerifiedDate, service.attain( pool1.getId() ).getLastVerified(), "Shoulda had date set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( pool2.getId() ).getLastVerified(), "Should notta had dates set initially.");

        service.updateDates( pool1.getId(), PoolAccessType.MODIFIED );
        pool1 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool1.getId() );
        pool2 = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( pool2.getId() );

        assertNotNull(service.attain( pool1.getId() ).getLastAccessed(), "Shoulda updated last accessed date and last modified date on pool1 only.");
        assertNotNull(service.attain( pool1.getId() ).getLastModified(), "Shoulda updated last accessed date and last modified date on pool1 only.");
        assertEquals(null, pool1.getLastVerified(), "Shoulda updated last accessed date and last modified date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastAccessed(), "Shoulda updated last accessed date and last modified date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastModified(), "Shoulda updated last accessed date and last modified date on pool1 only.");
        assertEquals(null, service.attain( pool2.getId() ).getLastVerified(), "Shoulda updated last accessed date and last modified date on pool1 only.");
    }
}
