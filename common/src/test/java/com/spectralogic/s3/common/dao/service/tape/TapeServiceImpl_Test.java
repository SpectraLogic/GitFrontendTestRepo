/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService.TapeAccessType;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class TapeServiceImpl_Test 
{
    @Test
    public void testValidStateTransitionsWork()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                tape.setLastCheckpoint( "cp" ).setTakeOwnershipPending( true ),
                Tape.LAST_CHECKPOINT, Tape.TAKE_OWNERSHIP_PENDING );
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");

        assertNotNull(tape.getPartitionId(), "Partition id should be set.");
        service.transistState( tape, TapeState.LOST );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.LOST, tape.getState(), "State transitions shoulda been correct.");
        assertNull(tape.getPartitionId(), "Partition id should be cleared when tape is lost.");

        service.transistState( tape, TapeState.EJECTED );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.EJECTED, tape.getState(), "State transitions shoulda been correct.");
        assertNull(tape.getPartitionId(), "Partition id should be cleared when tape is ejected.");

        service.transistState( tape, TapeState.PENDING_INSPECTION );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.UNKNOWN );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.UNKNOWN, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.FORMAT_PENDING );
        assertEquals(TapeState.UNKNOWN, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.FORMAT_PENDING, tape.getState(), "State transitions shoulda been correct.");

        service.rollbackLastStateTransition( tape );

        service.transistState( tape, TapeState.FORMAT_PENDING );
        assertEquals(TapeState.UNKNOWN, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.FORMAT_PENDING, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.FORMAT_IN_PROGRESS );
        assertEquals(TapeState.UNKNOWN, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.FORMAT_IN_PROGRESS, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.NORMAL );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.NORMAL, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.LOST );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.LOST, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.PENDING_INSPECTION );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.UNKNOWN );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.UNKNOWN, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.BAD );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.BAD, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.NORMAL );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.NORMAL, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.EJECT_TO_EE_IN_PROGRESS, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.EJECT_FROM_EE_PENDING );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.EJECT_FROM_EE_PENDING, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.EJECTED );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.EJECTED, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.LOST );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.LOST, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.EJECTED );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.EJECTED, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.OFFLINE );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.OFFLINE, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.ONLINE_IN_PROGRESS );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.ONLINE_IN_PROGRESS, tape.getState(), "State transitions shoulda been correct.");

        service.transistState( tape, TapeState.PENDING_INSPECTION );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");

        mockDaoDriver.attainAndUpdate( tape );
        assertEquals("cp", tape.getLastCheckpoint(), "State transitions thus far should notta reset last checkpoint.");
        assertTrue(tape.isTakeOwnershipPending(), "State transitions thus far should notta reset take ownership pending.");

        service.transistState( tape, TapeState.FOREIGN );
        mockDaoDriver.attainAndUpdate( tape );
        assertNull(tape.getLastCheckpoint(), "State transitions thus far shoulda reset last checkpoint.");
        assertFalse(tape.isTakeOwnershipPending(), "State transitions thus far shoulda reset take ownership pending.");
    }
    
    
    @Test
    public void testTransistStateDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        service.transistState( tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
        tape = service.attain( tape.getId() );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getPreviousState(), "Shoulda stored previous state since new state is cancellable.");
        assertEquals(TapeState.EJECT_TO_EE_IN_PROGRESS, tape.getState(), "Shoulda stored state transisted to.");

        service.rollbackLastStateTransition( tape );
        tape = service.attain( tape.getId() );
        assertEquals(null, tape.getPreviousState(), "Shoulda reverted to previous state.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "Shoulda reverted to previous state.");

        service.transistState( tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
        tape = service.attain( tape.getId() );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getPreviousState(), "Shoulda stored previous state since new state is cancellable.");
        assertEquals(TapeState.EJECT_TO_EE_IN_PROGRESS, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.UNKNOWN );
        tape = service.attain( tape.getId() );
        assertEquals(null, tape.getPreviousState(), "Should notta stored previous state since new state is not cancellable.");
        assertEquals(TapeState.UNKNOWN, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.FORMAT_PENDING );
        service.transistState( tape, TapeState.EJECTED );
        tape = service.attain( tape.getId() );
        assertEquals(TapeState.UNKNOWN, tape.getPreviousState(), "Shoulda stored previous state since valid previous state available.");
        assertEquals(TapeState.EJECTED, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.OFFLINE );
        service.transistState( tape, TapeState.PENDING_INSPECTION );
        assertEquals(TapeState.UNKNOWN, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.EJECT_TO_EE_IN_PROGRESS, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.NORMAL );
        assertEquals(null, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.NORMAL, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.EJECT_TO_EE_IN_PROGRESS );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.EJECT_TO_EE_IN_PROGRESS, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.EJECT_FROM_EE_PENDING );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.EJECT_FROM_EE_PENDING, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.EJECTED );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.EJECTED, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.LOST );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.LOST, tape.getState(), "Shoulda stored state transisted to.");

        service.transistState( tape, TapeState.EJECTED );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "Shoulda stored previous state since new state has previous tracking forced.");
        assertEquals(TapeState.EJECTED, tape.getState(), "Shoulda stored state transisted to.");
    }
    
    
    @Test
    public void testRollbackLastStateTransisitionDoesSoIfValid()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        
        final Tape [] tapeContainer = new Tape[] { tape };
        TestUtil.assertThrows( 
                "Not a cancellable state, so can't roll back.", 
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        service.rollbackLastStateTransition( tapeContainer[ 0 ] );
                    }
                } );

        service.transistState( tape, TapeState.EJECT_FROM_EE_PENDING );
        tape = service.attain( tape.getId() );
        assertEquals(TapeState.PENDING_INSPECTION, tape.getPreviousState(), "Shoulda stored previous state since new state is cancellable.");
        assertEquals(TapeState.EJECT_FROM_EE_PENDING, tape.getState(), "Shoulda stored state transisted to.");

        tapeContainer[ 0 ] = tape;
        dbSupport.getDataManager().updateBean(
                CollectionFactory.toSet( Tape.STATE, Tape.PREVIOUS_STATE ),
                tape.setState( TapeState.EJECT_FROM_EE_PENDING ).setPreviousState( null ) );
        TestUtil.assertThrows( 
                "No previous state, so can't rollback.", 
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        service.rollbackLastStateTransition( tapeContainer[ 0 ] );
                    }
                } );
        
        service.transistState( tape, TapeState.NORMAL );
        TestUtil.assertThrows( 
                "State is non-previous-state-tracking, so can't rollback.", 
                IllegalStateException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        service.rollbackLastStateTransition( tapeContainer[ 0 ] );
                    }
                } );
    }
    
    
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
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "partition");
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd1.getId(), partition.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd2.getId(), partition.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm3 = mockDaoDriver.addTapePartitionToStorageDomain(
                sd3.getId(), partition.getId(), TapeType.LTO5 );
        mockDaoDriver.createStorageDomain( "sd4" );

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
        
        // Tapes usable to b1 in sd1
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( null) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 0 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 1 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm1.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 2 ) ) );
        
        // Tapes usable to b1 in sd2
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm2.getId() )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 10 ) ) );
        
        // Tapes usable to b2 in sd1
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 100 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 200 ) ) );
        
        // Tapes usable to b2 in sd3
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm3.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) ) );
        
        // Unusable tapes since not normal state
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.EJECT_FROM_EE_PENDING )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm3.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 9 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.EJECT_FROM_EE_PENDING )
                .setFullOfData( false )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) ) );
        
        // Unusable tapes since eject pending
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setStorageDomainMemberId( sdm3.getId() )
                .setEjectPending( new Date() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 9 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setEjectPending( new Date() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) ) );
        
        // Unusable tapes since full of data
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( true )
                .setStorageDomainMemberId( sdm3.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 9 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( true )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) ) );
        
        // Unusable tapes since write protected
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setWriteProtected( true )
                .setStorageDomainMemberId( sdm3.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 9 ) ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Tape.class )
                .setBarCode( UUID.randomUUID().toString() )
                .setType( TapeType.values()[ 0 ] )
                .setState( TapeState.NORMAL )
                .setFullOfData( false )
                .setWriteProtected( true )
                .setBucketId( b1.getId() )
                .setPartitionId( partition.getId() )
                .setStorageDomainMemberId( sdm1.getId() )
                .setPartitionId( partition.getId() )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) ) );
        addBufferSpaceToTapes(dbSupport.getServiceManager());
        assertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 0, 0, 1, 2 },
                dbSupport.getServiceManager().getService( TapeService.class ).getAvailableSpacesForBucket(
                        b1.getId(), sd1.getId() ) );
        assertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 10 },
                dbSupport.getServiceManager().getService( TapeService.class ).getAvailableSpacesForBucket(
                        b1.getId(), sd2.getId() ) );
        assertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] {},
                dbSupport.getServiceManager().getService( TapeService.class ).getAvailableSpacesForBucket(
                        b1.getId(), sd3.getId() ) );
        
        assertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 100, 200 },
                dbSupport.getServiceManager().getService( TapeService.class ).getAvailableSpacesForBucket(
                        b2.getId(), sd1.getId() ) );
        assertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] {},
                dbSupport.getServiceManager().getService( TapeService.class ).getAvailableSpacesForBucket(
                        b2.getId(), sd2.getId() ) );
        assertArraysEqual(
                "Shoulda calculated correct total available raw capacity.",
                new long [] { 1000 },
                dbSupport.getServiceManager().getService( TapeService.class ).getAvailableSpacesForBucket(
                        b2.getId(), sd3.getId() ) );
    }
    
    
    private void assertArraysEqual(final String message, final long [] expected, final long [] actual )
    {
        assertEquals(expected.length,  actual.length, message);
        for ( int i = 0; i < actual.length; ++i )
        {
            assertEquals(expected[ i ],  actual[i], "At index " + i + ", " + message);
        }
    }
    
    
    private static void addBufferSpaceToTapes(final BeansServiceManager serviceManager) {
        serviceManager.getRetriever(Tape.class).retrieveAll().toSet().forEach( t -> {
            if (t.getAvailableRawCapacity() != null) {
                t.setAvailableRawCapacity( t.getAvailableRawCapacity() + PersistenceTargetUtil.RESERVED_SPACE_ON_TAPE);
                serviceManager.getUpdater(Tape.class).update(t, Tape.AVAILABLE_RAW_CAPACITY);
            }
        });
    }
    
    
    @Test
    public void testUpdatePreviousStateWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        service.transistState( tape, TapeState.NORMAL );
        service.updatePreviousState( tape, null );
        service.transistState( tape, TapeState.PENDING_INSPECTION );
        assertEquals(TapeState.NORMAL, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");

        service.updatePreviousState( tape, null );
        assertEquals(null, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");

        service.updatePreviousState( tape, null );
        
        service.transistState( tape, TapeState.NORMAL );
        service.transistState( tape, TapeState.PENDING_INSPECTION );
        service.updatePreviousState( tape, TapeState.UNKNOWN );
        assertEquals(TapeState.UNKNOWN, tape.getPreviousState(), "State transitions shoulda been correct.");
        assertEquals(TapeState.PENDING_INSPECTION, tape.getState(), "State transitions shoulda been correct.");
    }
    
    
    @Test
    public void testUpdatePreviousStateToNullWhenStateIsCancellableNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        service.transistState( tape, TapeState.NORMAL );
        service.transistState( tape, TapeState.FORMAT_PENDING );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.updatePreviousState( tape, null );
            }
        } );
    }
    
    
    @Test
    public void testUpdatePreviousStateToNonNullWhenStateIsNonPreviousStateTrackingNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        service.transistState( tape, TapeState.NORMAL );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.updatePreviousState( tape, TapeState.UNKNOWN );
            }
        } );
    }
    
    
    @Test
    public void testUpdatePreviousStateToIllegalPreviousStateNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        service.transistState( tape, TapeState.PENDING_INSPECTION );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.updatePreviousState( tape, TapeState.FORMAT_PENDING );
            }
        } );
    }
    
    
    @Test
    public void testUpdateStateExplicitlyNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.update( tape, Tape.STATE );
            }
        } );
    }
    
    
    @Test
    public void testUpdatePreviousStateExplicitlyNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                service.update( tape, Tape.PREVIOUS_STATE );
            }
        } );
    }
    
    
    @Test
    public void testUpdateDatesForVerifiedDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape1 = mockDaoDriver.createTape();
        Tape tape2 = mockDaoDriver.createTape();
        
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( Tape.class ).setVerifyPending( BlobStoreTaskPriority.values()[ 0 ] ), 
                Tape.VERIFY_PENDING );
        
        final Date lastVerifiedDate = new Date();
        mockDaoDriver.updateBean(
                tape1.setLastVerified( lastVerifiedDate ), PersistenceTarget.LAST_VERIFIED );
        tape1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() );
        tape2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() );

        assertEquals(null, service.attain( tape1.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape1.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(lastVerifiedDate, service.attain( tape1.getId() ).getLastVerified(), "Shoulda had date set initially.");
        assertNotNull(service.attain( tape1.getId() ).getVerifyPending(), "Shoulda had verify queued up initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastVerified(), "Should notta had dates set initially.");
        assertNotNull(service.attain( tape2.getId() ).getVerifyPending(), "Shoulda had verify queued up initially.");

        TestUtil.sleep( 2 );
        service.updateDates( tape1.getId(), TapeAccessType.VERIFIED );
        tape1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() );
        tape2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() );

        assertNotNull(service.attain( tape1.getId() ).getLastAccessed(), "Shoulda updated last verified/accessed date on tape1 only.");
        assertEquals(null, service.attain( tape1.getId() ).getLastModified(), "Shoulda updated last verified date on tape1 only.");
        assertFalse(lastVerifiedDate.equals( tape1.getLastVerified() ), "Shoulda updated last verified date on tape1 only.");
        assertNotNull(tape1.getLastVerified(), "Shoulda updated last verified date on tape1 only.");
        assertNull(service.attain( tape1.getId() ).getVerifyPending(), "Shoulda updated last verified date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastAccessed(), "Shoulda updated last verified date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastModified(), "Shoulda updated last verified date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastVerified(), "Shoulda updated last verified date on tape1 only.");
        assertNotNull(service.attain( tape2.getId() ).getVerifyPending(), "Shoulda updated last verified date on tape1 only.");
    }
    
    
    @Test
    public void testUpdateDatesForAccessedDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape1 = mockDaoDriver.createTape();
        Tape tape2 = mockDaoDriver.createTape();
        
        final Date lastVerifiedDate = new Date();
        mockDaoDriver.updateBean(
                tape1.setLastVerified( lastVerifiedDate ), PersistenceTarget.LAST_VERIFIED );
        tape1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() );
        tape2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() );

        assertEquals(null, service.attain( tape1.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape1.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(lastVerifiedDate, service.attain( tape1.getId() ).getLastVerified(), "Shoulda had date set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastVerified(), "Should notta had dates set initially.");

        service.updateDates( tape1.getId(), TapeAccessType.ACCESSED );
        tape1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() );
        tape2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() );

        assertNotNull(service.attain( tape1.getId() ).getLastAccessed(), "Shoulda updated last accessed date on tape1 only.");
        assertEquals(null, service.attain( tape1.getId() ).getLastModified(), "Shoulda updated last accessed date on tape1 only.");
        assertEquals(lastVerifiedDate, tape1.getLastVerified(), "Shoulda updated last accessed date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastAccessed(), "Shoulda updated last accessed date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastModified(), "Shoulda updated last accessed date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastVerified(), "Shoulda updated last accessed date on tape1 only.");
    }
    
    
    @Test
    public void testUpdateDatesForModifiedDoesSo()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        Tape tape1 = mockDaoDriver.createTape();
        Tape tape2 = mockDaoDriver.createTape();

        final Date lastVerifiedDate = new Date();
        mockDaoDriver.updateBean(
                tape1.setLastVerified( lastVerifiedDate ), PersistenceTarget.LAST_VERIFIED );
        tape1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() );
        tape2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() );

        assertEquals(null, service.attain( tape1.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape1.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(lastVerifiedDate, service.attain( tape1.getId() ).getLastVerified(), "Shoulda had date set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastAccessed(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastModified(), "Should notta had dates set initially.");
        assertEquals(null, service.attain( tape2.getId() ).getLastVerified(), "Should notta had dates set initially.");

        service.updateDates( tape1.getId(), TapeAccessType.MODIFIED );
        tape1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape1.getId() );
        tape2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape2.getId() );

        assertNotNull(service.attain( tape1.getId() ).getLastAccessed(), "Shoulda updated last accessed date and last modified date on tape1 only.");
        assertNotNull(service.attain( tape1.getId() ).getLastModified(), "Shoulda updated last accessed date and last modified date on tape1 only.");
        assertEquals(null, tape1.getLastVerified(), "Shoulda updated last accessed date and last modified date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastAccessed(), "Shoulda updated last accessed date and last modified date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastModified(), "Shoulda updated last accessed date and last modified date on tape1 only.");
        assertEquals(null, service.attain( tape2.getId() ).getLastVerified(), "Shoulda updated last accessed date and last modified date on tape1 only.");
    }
    
    
    @Test
    public void testDeassociateFromPartitionDeassociatesTheSpecifiedTapesOnly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
            
        final TapeService service = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );

        assertEquals(0,  service.getCount(Require.beanPropertyEquals(Tape.PARTITION_ID, null)), "All tapes shoulda been in partition initially.");
        assertEquals(5,  service.getCount(Require.beanPropertyEquals(Tape.STATE, TapeState.NORMAL)), "All tapes shoulda been in normal state initially.");
        service.deassociateFromPartition( CollectionFactory.toSet( tape1.getId(), tape2.getId() ) );
        assertEquals(2,  service.getCount(Require.beanPropertyEquals(Tape.PARTITION_ID, null)), "All non-deassociated tapes shoulda been in partition.");
        assertEquals(2,  service.getCount(Require.beanPropertyEquals(Tape.STATE, TapeState.LOST)), "All deassociated tapes shoulda been transisted to LOST state.");
        assertEquals(3,  service.getCount(Require.beanPropertyEquals(Tape.STATE, TapeState.NORMAL)), "All deassociated tapes shoulda been transisted to LOST state.");
        assertEquals(2,  service.getCount(Require.beanPropertyEquals(
                Tape.PREVIOUS_STATE, TapeState.PENDING_INSPECTION)), "All deassociated tapes shoulda been transisted to pending inspection prev state.");
    }
}
