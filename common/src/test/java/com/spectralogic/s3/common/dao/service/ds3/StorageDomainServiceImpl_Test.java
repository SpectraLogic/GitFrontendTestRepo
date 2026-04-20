/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainCapacitySummary;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.SystemCapacitySummary;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class StorageDomainServiceImpl_Test 
{
    @Test
    public void testGetStorageDomainCapacitySummaryReturnsSummaryWhenTapeStorageUsed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final TapePartition partition = BeanFactory.newBean( TapePartition.class )
                .setName( "p" ).setSerialNumber( "sn" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values() [ 0 ] )
                .setQuiesced( Quiesced.NO );
        dbSupport.getDataManager().createBean( partition ); 
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), partition.getId(), TapeType.values()[ 0 ] );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition.getId(), TapeType.values()[ 0 ] );
                
        final Tape t1 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity(10000L)
                .setTotalRawCapacity(15000L)
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t1 );
        final Tape t2 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity(10000L)
                .setTotalRawCapacity(15000L)
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t2 );
        final Tape t3 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity(10000L)
                .setTotalRawCapacity(15000L)
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "c" )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity(100000L)
                .setTotalRawCapacity(150000L)
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "d" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t4 );
        final Tape t5 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity(1L)
                .setTotalRawCapacity(2L)
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "e" )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t5 );
        final Tape t6 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity(100L)
                .setTotalRawCapacity(100L)
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "f" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t6 );
        
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );

        verifyNoStorage( service.getCapacitySummary( sd1.getId(), null, null ).getPool() );
        StorageDomainCapacitySummary summary = 
                service.getCapacitySummary( sd1.getId(), null, null ).getTape();
        assertEquals(
                10000,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                20000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                30000,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                10000,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
              );
        assertEquals(
                20000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                30000,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(10000,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(20000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(30000,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( sd2.getId(), null, null ).getPool() );
        summary = service.getCapacitySummary( sd2.getId(), null, null ).getTape();
        assertEquals(
                5001,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                10000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                15002,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                5001,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                10000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                15002,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(5001,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(10000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(15002,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( sd3.getId(), null, null ).getPool() );
        summary = service.getCapacitySummary( sd3.getId(), null, null ).getTape();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( null, null ).getPool() );
        summary = service.getCapacitySummary( null, null ).getTape();
        assertEquals(
                15001,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                30000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                45002,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
               );
        assertEquals(
                0,
                ( (SystemCapacitySummary)summary ).getPhysicalAvailable(),
                "Shoulda calculated capacity summary correctly."
                );
        
        assertEquals(
                15001,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                30000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                45002,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                0,
                ( (SystemCapacitySummary)summary ).getPhysicalAvailable(),
                "Shoulda calculated capacity summary correctly."
                );
        
        assertEquals(15001,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(30000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(45002,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  ((SystemCapacitySummary) summary).getPhysicalAvailable(), "Shoulda calculated capacity summary correctly.");

        summary = service.getCapacitySummary( 
                Require.beanPropertyEquals( Tape.TYPE, TapeType.values()[ 0 ] ), null ).getTape();
        assertEquals(
                15001,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                30000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                45002,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                0,
                ( (SystemCapacitySummary)summary ).getPhysicalAvailable(),
                "Shoulda calculated capacity summary correctly."
                );
        
        assertEquals(
                15001,
                summary.getPhysicalUsed(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                30000,
                summary.getPhysicalFree(),
                "Shoulda calculated capacity summary correctly."
                );
        assertEquals(
                45002,
                summary.getPhysicalAllocated(),
                "Shoulda calculated capacity summary correctly."
                 );
        assertEquals(
                0,
                ( (SystemCapacitySummary)summary ).getPhysicalAvailable(),
                "Shoulda calculated capacity summary correctly."
                );
        
        assertEquals(15001,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(30000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(45002,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  ((SystemCapacitySummary) summary).getPhysicalAvailable(), "Shoulda calculated capacity summary correctly.");

        summary = service.getCapacitySummary( 
                Require.beanPropertyEquals( Tape.TYPE, TapeType.values()[ 1 ] ), null ).getTape();
        verifyNoStorage( summary );
    }
    
    
    @Test
    public void testGetStorageDomainCapacitySummaryReturnsSummaryWhenPoolStorageUsed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final PoolPartition partition = BeanFactory.newBean( PoolPartition.class )
                .setName( "p" ).setType( PoolType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( partition );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd1.getId(), partition.getId() );
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), partition.getId() );
                
        final Pool p1 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p1 );
        final Pool p2 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p2 );
        final Pool p3 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p3 );
        final Pool p4 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 10000 )
                .setTotalCapacity( 15000 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p4 );
        final Pool p5 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.FOREIGN )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1 )
                .setTotalCapacity( 2 )
                .setType( PoolType.values()[ 0 ] )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p5 );
        final Pool p6 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.LOST )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 99999 )
                .setTotalCapacity( 99999 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p6 );
        
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );

        verifyNoStorage( service.getCapacitySummary( sd1.getId(), null, null ).getTape() );
        StorageDomainCapacitySummary summary = 
                service.getCapacitySummary( sd1.getId(), null, null ).getPool();
        assertEquals(1000,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(2000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(3000,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( sd2.getId(), null, null ).getTape() );
        summary = service.getCapacitySummary( sd2.getId(), null, null ).getPool();
        assertEquals(501,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1502,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( sd3.getId(), null, null ).getTape() );
        summary = service.getCapacitySummary( sd3.getId(), null, null ).getPool();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( null, null ).getTape() );
        summary = service.getCapacitySummary( null, null ).getPool();
        assertEquals(1501,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(3000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(4502,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
        assertEquals(15000,  ((SystemCapacitySummary) summary).getPhysicalAvailable(), "Shoulda calculated capacity summary correctly.");

        summary = service.getCapacitySummary( null, Require.beanPropertyEquals( 
                PoolObservable.HEALTH, PoolHealth.values()[ 0 ] ) ).getPool();
        assertEquals(1501,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(3000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(4502,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
        assertEquals(15000,  ((SystemCapacitySummary) summary).getPhysicalAvailable(), "Shoulda calculated capacity summary correctly.");

        summary = service.getCapacitySummary( null, Require.beanPropertyEquals( 
                PoolObservable.HEALTH, PoolHealth.values()[ 1 ] ) ).getPool();
        verifyNoStorage( summary );
    }
    
    
    @Test
    public void testGetStandardIsolatedBucketCapacitySummaryReturnsSummaryWhenTapeStorageUsed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final TapePartition partition = BeanFactory.newBean( TapePartition.class )
                .setName( "p" ).setSerialNumber( "sn" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values() [ 0 ] )
                .setQuiesced( Quiesced.NO );
        dbSupport.getDataManager().createBean( partition );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), partition.getId(), TapeType.values()[ 0 ] );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition.getId(), TapeType.values()[ 0 ] );
        
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd1.getId() );
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
                
        final Tape t1 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t1 );
        final Tape t2 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t2 );
        final Tape t3 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "c" )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 10000 ) )
                .setTotalRawCapacity( Long.valueOf( 15000 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "d" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t4 );
        final Tape t5 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity( Long.valueOf( 1 ) )
                .setTotalRawCapacity( Long.valueOf( 2 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "e" )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t5 );
        final Tape t6 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) )
                .setTotalRawCapacity( Long.valueOf( 99999 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "f" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t6 );
        
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getPool() );
        StorageDomainCapacitySummary summary = 
                service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getTape();
        assertEquals(500,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1500,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getPool() );
        summary = service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getTape();
        assertEquals(1,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(2,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getPool() );
        summary = service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getTape();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
    }
    
    
    @Test
    public void testGetStandardIsolatedBucketCapacitySummaryReturnsSummaryWhenPoolStorageUsed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy =
                mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final PoolPartition partition = BeanFactory.newBean( PoolPartition.class )
                .setName( "p" ).setType( PoolType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( partition );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd1.getId(), partition.getId() );
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), partition.getId() );
        
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.STANDARD, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd1.getId() );
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
                
        final Pool p1 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p1 );
        final Pool p2 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p2 );
        final Pool p3 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p3 );
        final Pool p4 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 10000 )
                .setTotalCapacity( 15000 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p4 );
        final Pool p5 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.FOREIGN )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1 )
                .setTotalCapacity( 2 )
                .setType( PoolType.values()[ 0 ] )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p5 );
        final Pool p6 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.LOST )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 99999 )
                .setTotalCapacity( 99999 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p6 );
        
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getTape() );
        StorageDomainCapacitySummary summary = 
                service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getPool();
        assertEquals(500,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1500,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getTape() );
        summary = service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getPool();
        assertEquals(1,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(2,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getTape() );
        summary = service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getPool();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
    }
    
    
    @Test
    public void testGetBucketIsolatedBucketCapacitySummaryReturnsSummaryWhenTapeStorageUsed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = 
                mockDaoDriver.createDataPolicy( MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final TapePartition partition = BeanFactory.newBean( TapePartition.class )
                .setName( "p" ).setSerialNumber( "sn" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values() [ 0 ] )
                .setQuiesced( Quiesced.NO );
        dbSupport.getDataManager().createBean( partition );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), partition.getId(), TapeType.values()[ 0 ] );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition.getId(), TapeType.values()[ 0 ] );
        
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd3.getId() );
                
        final Tape t1 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "a" )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t1 );
        final Tape t2 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t2 );
        final Tape t3 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "c" )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 10000 ) )
                .setTotalRawCapacity( Long.valueOf( 15000 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "d" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t4 );
        final Tape t5 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity( Long.valueOf( 1 ) )
                .setTotalRawCapacity( Long.valueOf( 2 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "e" )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t5 );
        final Tape t6 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) )
                .setTotalRawCapacity( Long.valueOf( 99999 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "f" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t6 );
        
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getPool() );
        StorageDomainCapacitySummary summary = 
                service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getTape();
        assertEquals(500,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1000,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1500,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getPool() );
        summary = service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getTape();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getPool() );
        summary = service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getTape();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
    }
    
    
    @Test
    public void testGetBucketIsolatedBucketCapacitySummaryReturnsSummaryWhenPoolStorageUsed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final PoolPartition partition = BeanFactory.newBean( PoolPartition.class )
                .setName( "p" ).setType( PoolType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( partition );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd1.getId(), partition.getId() );
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd2.getId(), partition.getId() );
        
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(),
                DataPersistenceRuleType.TEMPORARY, 
                sd3.getId() );
                
        final Pool p1 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p1 );
        final Pool p2 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 990 )
                .setReservedCapacity( 10 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p2 );
        final Pool p3 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1000 )
                .setTotalCapacity( 1500 )
                .setType( PoolType.values()[ 0 ] )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p3 );
        final Pool p4 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.NORMAL )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 10000 )
                .setTotalCapacity( 15000 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p4 );
        final Pool p5 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.FOREIGN )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 1 )
                .setTotalCapacity( 2 )
                .setType( PoolType.values()[ 0 ] )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p5 );
        final Pool p6 = BeanFactory.newBean( Pool.class )
                .setState( PoolState.LOST )
                .setName( UUID.randomUUID().toString() )
                .setMountpoint( UUID.randomUUID().toString() )
                .setHealth( PoolHealth.values()[ 0 ] )
                .setGuid( UUID.randomUUID().toString() )
                .setAvailableCapacity( 99999 )
                .setTotalCapacity( 99999 )
                .setType( PoolType.values()[ 0 ] )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( p6 );
        
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getTape() );
        StorageDomainCapacitySummary summary = 
                service.getCapacitySummary( bucket1.getId(), sd1.getId(), null, null ).getPool();
        assertEquals(500,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(990,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(1500,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getTape() );
        summary = service.getCapacitySummary( bucket1.getId(), sd2.getId(), null, null ).getPool();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");

        verifyNoStorage( service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getTape() );
        summary = service.getCapacitySummary( bucket1.getId(), sd3.getId(), null, null ).getPool();
        assertEquals(0,  summary.getPhysicalUsed(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalFree(), "Shoulda calculated capacity summary correctly.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Shoulda calculated capacity summary correctly.");
    }
    
    
    @Test
    public void testBothUuidAndNameCanBeUsedToRetrieveStorageDomain()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainService service = dbSupport.getServiceManager().getService( StorageDomainService.class );
        final StorageDomain attainedStorageDomain1 = service.attain( storageDomain.getName() );
        final StorageDomain attainedStorageDomain2 = service.attain( storageDomain.getId().toString() );
        final StorageDomain retrievedStorageDomain1 = service.retrieve( storageDomain.getName() );
        final StorageDomain retrievedStorageDomain2 = service.retrieve( storageDomain.getId().toString() );
        
        assertNotNull(
                storageDomain.getId(),
                "Id should not have been null!" );
        final Object expected3 = storageDomain.getId();
        assertEquals(expected3, attainedStorageDomain1.getId(), "Should have retrieved storage domain!");
        final Object expected2 = storageDomain.getId();
        assertEquals(expected2, attainedStorageDomain2.getId(), "Should have retrieved storage domain!");
        final Object expected1 = storageDomain.getId();
        assertEquals(expected1, retrievedStorageDomain1.getId(), "Should have retrieved storage domain!");
        final Object expected = storageDomain.getId();
        assertEquals(expected, retrievedStorageDomain2.getId(), "Should have retrieved storage domain!");

    }
    
    
    @Test
    public void testSelectAppropriateStorageDomainMemberDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );
        
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd" );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition1.getId(), TapeType.LTO5);
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition2.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm3 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition1.getId(), TapeType.LTO6);
        final StorageDomainMember sdm4 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition2.getId(), TapeType.LTO6 );
        
        final Tape t1 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        final Tape t2 = mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO5 );
        final Tape t3 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO6 );
        final Tape t4 = mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO6 );

        final Object expected3 = sdm1.getId();
        assertEquals(expected3, service.selectAppropriateStorageDomainMember( t1, sd.getId() ), "Should have selected appropriate storage domain member");

        final Object expected2 = sdm2.getId();
        assertEquals(expected2, service.selectAppropriateStorageDomainMember( t2, sd.getId() ), "Should have selected appropriate storage domain member");

        final Object expected1 = sdm3.getId();
        assertEquals(expected1, service.selectAppropriateStorageDomainMember( t3, sd.getId() ), "Should have selected appropriate storage domain member");

        final Object expected = sdm4.getId();
        assertEquals(expected, service.selectAppropriateStorageDomainMember( t4, sd.getId() ), "Should have selected appropriate storage domain member");
    }
    
    
    @Test
    public void testSelectAppropriateStorageDomainThrowsExceptionWhenNoneAppropriate()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomainService service = 
                dbSupport.getServiceManager().getService( StorageDomainService.class );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );
        
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd" );
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition1.getId(), TapeType.LTO5);
        mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), partition2.getId(), TapeType.LTO6 );
        
        
        final Tape t1 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO6 );
        final Tape t2 = mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO5 );
               
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.selectAppropriateStorageDomainMember( t1, sd.getId() );
                }
            } );
        
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.selectAppropriateStorageDomainMember( t2, sd.getId() );
                }
            } );
                
    }
    
    
    private void verifyNoStorage( final StorageDomainCapacitySummary summary )
    {
        assertEquals(0,  summary.getPhysicalUsed(), "Should notta been any pool storage.");
        assertEquals(0,  summary.getPhysicalFree(), "Should notta been any pool storage.");
        assertEquals(0,  summary.getPhysicalAllocated(), "Should notta been any pool storage.");
        if ( SystemCapacitySummary.class.isAssignableFrom( summary.getClass() ) )
        {
            assertEquals(0,  ((SystemCapacitySummary) summary).getPhysicalAvailable(), "Should notta been any pool storage.");
        }
    }
}
