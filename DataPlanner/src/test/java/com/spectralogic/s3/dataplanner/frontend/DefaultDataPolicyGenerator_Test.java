/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.DataPersistenceRuleService;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainMemberService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class DefaultDataPolicyGenerator_Test 
{
    @Test
    public void testConstructorNullResourceNotAllowed()
    {
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new DefaultDataPolicyGenerator(
                null,
                dbSupport.getServiceManager(),
                1000 ) );
    }
    
    
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new DefaultDataPolicyGenerator(
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                null,
                1000 ) );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        
        new DefaultDataPolicyGenerator(
                InterfaceProxyFactory.getProxy( DataPolicyManagementResource.class, null ),
                dbSupport.getServiceManager(),
                1000 ).shutdown();
    }
    

    @Test
    public void testTapeDataPoliciesCreatedOnceTapeComesOnline()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DefaultDataPolicyGenerator generator = new DefaultDataPolicyGenerator(
                getDataPolicyManagementResource( dbSupport ),
                dbSupport.getServiceManager(),
                20 );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "tp2" );
        final TapePartition partition3 = mockDaoDriver.createTapePartition( null, "tp3" );
        mockDaoDriver.updateBean( partition1.setState( TapePartitionState.ERROR )
                                            .setDriveType( TapeDriveType.LTO5 ), TapePartition.STATE,
                TapePartition.DRIVE_TYPE );
        mockDaoDriver.updateBean( partition2.setState( TapePartitionState.OFFLINE )
                                            .setDriveType( TapeDriveType.LTO6 ), TapePartition.STATE,
                TapePartition.DRIVE_TYPE );
        mockDaoDriver.updateBean( partition3.setState( TapePartitionState.OFFLINE )
                                            .setDriveType( TapeDriveType.LTO7 ), TapePartition.STATE,
                TapePartition.DRIVE_TYPE );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO_CLEANING_TAPE );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO_CLEANING_TAPE );
        mockDaoDriver.createTape( partition3.getId(), TapeState.BAR_CODE_MISSING, TapeType.UNKNOWN );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        dbSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( TapePartition.STATE ),
                BeanFactory.newBean( TapePartition.class ).setState( TapePartitionState.ONLINE ),
                Require.nothing() );
        
        int i = 100;
        while ( --i > 0 && 5 != dbSupport.getServiceManager().getRetriever(
                DataPersistenceRule.class ).getCount() )
        {
            TestUtil.sleep( 10 );
        }
        if ( 0 >= i )
        {
            Assertions.fail( "Never created persistence rules." );
        }
        
        final DataPolicy dbBackupDp = dbSupport.getServiceManager().getRetriever( DataPolicy.class ).attain(
                NameObservable.NAME, DefaultDataPolicyGenerator.DATA_POLICY_DB_BACKUP );
        assertFalse(dbBackupDp.isBlobbingEnabled(), "Shoulda disabled blobbing.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(DataPersistenceRule.class).getCount(
                DataPlacement.DATA_POLICY_ID, dbBackupDp.getId()), "Shoulda created db backup dp propertly.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(DataPersistenceRule.class).getCount(
                DataPersistenceRule.ISOLATION_LEVEL, DataIsolationLevel.BUCKET_ISOLATED), "Shoulda created db backup dp propertly.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(
                StorageDomain.SECURE_MEDIA_ALLOCATION, Boolean.TRUE), "Shoulda created db backup dp propertly.");
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Shoulda created storage domains.");
        assertEquals(12,  dbSupport.getServiceManager()
                .getRetriever(
                        StorageDomainMember.class)
                .getCount(), "Shoulda created storage domains members.");

        generator.shutdown();
    }
    
    
    @Test
    public void testTapeDataPoliciesCorrectStorageDomainMemberCounts()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "tp1" );
        mockDaoDriver.updateBean( partition1.setDriveType( TapeDriveType.LTO7 ), TapePartition.DRIVE_TYPE );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO7 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO8 );
        
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "tp2" );
        mockDaoDriver.updateBean( partition2.setDriveType( TapeDriveType.LTO8 ), TapePartition.DRIVE_TYPE );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO7 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO8 );
        
        final DefaultDataPolicyGenerator generator =
                new DefaultDataPolicyGenerator( getDataPolicyManagementResource( dbSupport ),
                        dbSupport.getServiceManager(), 20 );
        
        int i = 100;
        while ( --i > 0 && 5 != dbSupport.getServiceManager()
                                         .getRetriever( DataPersistenceRule.class )
                                         .getCount() )
        {
            TestUtil.sleep( 10 );
        }

        assertEquals(3,  dbSupport.getServiceManager()
                .getRetriever(DataPolicy.class)
                .getCount(), "should have created 3 data policies.");

        assertEquals(5,  dbSupport.getServiceManager()
                .getRetriever(DataPersistenceRule.class)
                .getCount(), "should have created 4 data persistence rules.");

        assertEquals(4,  dbSupport.getServiceManager()
                .getRetriever(StorageDomain.class)
                .getCount(), "should have created 4 storage domains.");
        assertEquals(16,  dbSupport.getServiceManager()
                .getRetriever(
                        StorageDomainMember.class)
                .getCount(), "should have created 16 storage domains members.");
        assertEquals(0,  dbSupport.getServiceManager()
                .getRetriever(
                        StorageDomainMember.class)
                .getCount(
                        Require.beanPropertyEquals(
                                StorageDomainMember.WRITE_PREFERENCE,
                                WritePreferenceLevel.NEVER_SELECT)), "should have created 0 storage domains members with NEVER_SELECT");

        generator.shutdown();
    }
    

    @Test
    public void testPoolDataPoliciesCreatedOncePoolComesOnline()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DefaultDataPolicyGenerator generator = new DefaultDataPolicyGenerator(
                getDataPolicyManagementResource( dbSupport ),
                dbSupport.getServiceManager(),
                20 );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        final PoolPartition partition3 = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp1" );
        mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "dp2" );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        dbSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( PoolPartition.TYPE ),
                BeanFactory.newBean( PoolPartition.class ).setType( PoolType.NEARLINE ),
                Require.nothing() );
        
        int i = 100;
        while ( --i > 0 && 1 != dbSupport.getServiceManager().getRetriever(
                DataPersistenceRule.class ).getCount() )
        {
            TestUtil.sleep( 10 );
        }
        if ( 0 >= i )
        {
            Assertions.fail( "Never created persistence rules." );
        }
        generator.shutdown();
    }
    

    @Test
    public void testAllDataPoliciesCreatedOncePoolAndTapeComeOnline()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final DefaultDataPolicyGenerator generator = new DefaultDataPolicyGenerator(
                getDataPolicyManagementResource( dbSupport ),
                dbSupport.getServiceManager(),
                20 );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "tp2" );
        mockDaoDriver.updateBean( partition1.setState( TapePartitionState.ERROR )
                                            .setDriveType( TapeDriveType.LTO5 ), TapePartition.STATE,
                TapePartition.DRIVE_TYPE );
        mockDaoDriver.updateBean( partition2.setState( TapePartitionState.OFFLINE )
                                            .setDriveType( TapeDriveType.LTO6 ), TapePartition.STATE,
                TapePartition.DRIVE_TYPE );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL, TapeType.LTO_CLEANING_TAPE );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO5 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL, TapeType.LTO_CLEANING_TAPE );
        final PoolPartition partition3 = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp1" );
        mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "dp2" );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        dbSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( TapePartition.STATE ),
                BeanFactory.newBean( TapePartition.class ).setState( TapePartitionState.ONLINE ),
                Require.nothing() );
        dbSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( PoolPartition.TYPE ),
                BeanFactory.newBean( PoolPartition.class ).setType( PoolType.NEARLINE ),
                Require.nothing() );
        
        final PoolPartition partition4 = mockDaoDriver.createPoolPartition( PoolType.ONLINE, "dp3" );
        mockDaoDriver.createPool( partition4.getId(), PoolState.NORMAL );
        
        int i = 100;
        while ( --i > 0 && 11 != dbSupport.getServiceManager().getRetriever(
                DataPersistenceRule.class ).getCount() )
        {
            TestUtil.sleep( 10 );
        }
        if ( 0 >= i )
        {
            Assertions.fail( "Never created persistence rules." );
        }
        
        final StorageDomainService sdService =
                dbSupport.getServiceManager().getService( StorageDomainService.class );
        final StorageDomainMemberService sdmService =
                dbSupport.getServiceManager().getService( StorageDomainMemberService.class );
        final DataPolicyService dpService =
                dbSupport.getServiceManager().getService( DataPolicyService.class );
        final DataPersistenceRuleService dprService =
                dbSupport.getServiceManager().getService( DataPersistenceRuleService.class );
        
        final StorageDomain sdTape1 = sdService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.STORAGE_DOMAIN_TAPE_SINGLE_COPY_NAME );
        assertEquals(3,  sdmService.getCount(StorageDomainMember.STORAGE_DOMAIN_ID, sdTape1.getId()), "Shoulda generated members for every tape partition with tape in it.");

        final StorageDomain sdTape2 = sdService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.STORAGE_DOMAIN_TAPE_DUAL_COPY_NAME );
        assertEquals(3,  sdmService.getCount(StorageDomainMember.STORAGE_DOMAIN_ID, sdTape2.getId()), "Shoulda generated members for every tape partition with tape in it.");

        final StorageDomain sdPool = sdService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.STORAGE_DOMAIN_POOL_NAME );
        assertEquals(2,  sdmService.getCount(StorageDomainMember.STORAGE_DOMAIN_ID, sdPool.getId()), "Shoulda generated members for every pool partition of archive type.");

        final DataPolicy dpTape1 = dpService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.DATA_POLICY_TAPE_SINGLE_COPY_NAME );
        assertEquals(1,  dprService.getCount(DataPlacement.DATA_POLICY_ID, dpTape1.getId()), "Shoulda generated persistence rules correctly for each data policy.");

        final DataPolicy dpTape2 = dpService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.DATA_POLICY_TAPE_DUAL_COPY_NAME );
        assertEquals(2,  dprService.getCount(DataPlacement.DATA_POLICY_ID, dpTape2.getId()), "Shoulda generated persistence rules correctly for each data policy.");

        final DataPolicy dpPool = dpService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.DATA_POLICY_POOL_NAME );
        assertEquals(1,  dprService.getCount(DataPlacement.DATA_POLICY_ID, dpPool.getId()), "Shoulda generated persistence rules correctly for each data policy.");

        final DataPolicy dpHybrid1 = dpService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.DATA_POLICY_HYBRID_SINGLE_COPY_NAME );
        assertEquals(2,  dprService.getCount(DataPlacement.DATA_POLICY_ID, dpHybrid1.getId()), "Shoulda generated persistence rules correctly for each data policy.");

        final DataPolicy dpHybrid2 = dpService.attain( 
                NameObservable.NAME,
                DefaultDataPolicyGenerator.DATA_POLICY_HYBRID_DUAL_COPY_NAME );
        assertEquals(3,  dprService.getCount(DataPlacement.DATA_POLICY_ID, dpHybrid2.getId()), "Shoulda generated persistence rules correctly for each data policy.");

        generator.shutdown();
    }
    

    @Test
    public void testPoolDataPoliciesNeverGeneratedForSecondTime()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final PoolPartition partition3 = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "dp1" );
        mockDaoDriver.createPool( partition3.getId(), PoolState.NORMAL );
        
        DefaultDataPolicyGenerator generator = new DefaultDataPolicyGenerator(
                getDataPolicyManagementResource( dbSupport ),
                dbSupport.getServiceManager(),
                20 );
        
        int i = 100;
        while ( --i > 0 && 1 != dbSupport.getServiceManager().getRetriever(
                DataPersistenceRule.class ).getCount() )
        {
            TestUtil.sleep( 10 );
        }
        if ( 0 >= i )
        {
            Assertions.fail( "Never created persistence rules." );
        }
        generator.shutdown();
        dbSupport.getDataManager().deleteBeans( DataPersistenceRule.class, Require.nothing() );
        dbSupport.getDataManager().deleteBeans( DataPolicy.class, Require.nothing() );
        dbSupport.getDataManager().deleteBeans( StorageDomainMember.class, Require.nothing() );
        dbSupport.getDataManager().deleteBeans( StorageDomain.class, Require.nothing() );
        
        generator = new DefaultDataPolicyGenerator(
                getDataPolicyManagementResource( dbSupport ),
                dbSupport.getServiceManager(),
                20 );
        TestUtil.sleep( 50 );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomain.class).getCount(), "Should notta generated anything.");

        generator.shutdown();
    }
    
    
    private DataPolicyManagementResource getDataPolicyManagementResource( final DatabaseSupport dbSupport )
    {
        return new DataPolicyManagementResourceImpl( 
                InterfaceProxyFactory.getProxy( RpcServer.class, null ),
                dbSupport.getServiceManager() );
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
