/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public final class FormatTapeTask_Test
{
    @Test
    public void testNonSpectraTapeTypeGetsCorrectedByRunningTask()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual = tapeService.attain( tape.getId() ).getType();
        assertEquals(
                TapeType.LTO5,
                actual,
                "Shoulda corrected tape type.");
    }


    @Test
    public void testWrongTapeTypeGetsCorrectedByRunningTask()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive =
                mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.LTO6 ), Tape.TYPE );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual = tapeService.attain( tape.getId() ).getType();
        assertEquals(
                TapeType.LTO5,
                 actual,
                "Shoulda corrected tape type.");
    }
    
    
    @Test
    public void testRunForTapeNotInFormatPendingStateNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        TestUtil.assertThrows(
                "Tape state shoulda resulted in exception.",
                BlobStoreTaskNoLongerValidException.class,
                () -> task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() )
                        );
        final Object actual = task.getState();
        assertEquals(
                BlobStoreTaskState.COMPLETED,
                actual,
                "Shoulda updated tape task state to completed.");
    }
    
    @Test
    public void testRunForTapeThatIsWriteProtected()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setFailGetLoadedTapeInformation( true );
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(
                  true,
                tapeDriveResource.isFormatInvoked(),
                "Shoulda made format request.");
    }
    
    
    @Test
    public void testPrepareToRunThenFailedToRunResultsInCorrectTapeStateUpdates()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        mockDaoDriver.nullOutCapacityStats( tape );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        
        final String tn = Thread.currentThread().getName();
        
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        final Object actual3 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_IN_PROGRESS,
                actual3,
                "Shoulda updated state to in progress.");
        task.executionFailed( new RuntimeException("Boom") );
        
        Thread.currentThread().setName( tn );

        final Object actual2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                TapeState.FORMAT_PENDING,
                actual2,
                "Shoulda updated state to its original state.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
            .getPreviousState();
        assertEquals(
               TapeState.UNKNOWN,
                actual1,
                "Should notta lost previous state of tape." );
        final Object actual = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
                 0,
                actual,
                "Should notta recorded failure.");
    }
    
    
    @Test
    public void testRunWithFailureToUpdateTapeExtendedInformationFailsFormat()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setTakeOwnershipPending( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT, Tape.TAKE_OWNERSHIP_PENDING,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        final Object actual2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                TapeState.FORMAT_IN_PROGRESS,
                actual2,
                "Shoulda updated state to in progress.");

        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitUnchecked( task ) );

        assertEquals(
                true,
                tapeDriveResource.isFormatInvoked(),
                "Shoulda made format request.");
        final Object expected = tape.getId();
        final Object expected1 =  tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId();
        assertEquals(
                 expected,
                expected1,
                "Shoulda updated tape ownership." );
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_PENDING,
                 actual1,
                "Should notta updated state to normal.");
        final Object actual = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
                 2,
                 actual,
                "Shoulda recorded failures.");
    }
    
    
    @Test
    public void testRunWithoutErrorsUpdatesTapeStateAndMakesAppropriateCallsToTapeDriveResource()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setTakeOwnershipPending( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT, Tape.TAKE_OWNERSHIP_PENDING,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );
        final Object actual9 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                TapeState.FORMAT_IN_PROGRESS,
                actual9,
                "Shoulda updated state to in progress." );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(
                 true,
                tapeDriveResource.isFormatInvoked(),
                "Shoulda made format request.");
        final Object expected = tape.getId();
        final Object expected1 =  tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId();
        assertEquals(
                 expected,
                 expected1,
                "Shoulda updated tape ownership." );
        final Object actual8 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                TapeState.NORMAL,
                 actual8,
                "Shoulda updated state to normal.");
        final Object actual7 = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
                0,
                 actual7,
                "Should notta recorded failure.");
        final Object actual6 = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
                0,
                actual6,
                "Successful format shoulda wiped out previous tape failures.");
        final Object actual5 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isFullOfData();
        assertEquals(
                 false,
                actual5,
                "Successful format shoulda cleared full of data flag." );
        final Object actual4 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isTakeOwnershipPending();
        assertEquals(
               false,
                actual4,
                "Successful format shoulda cleared take ownership pending flag.");
        final Object actual3 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isAssignedToStorageDomain();
        assertEquals(
                false,
                actual3,
                "Successful format shoulda cleared assigned to bucket flag.");
        final Object actual2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getStorageDomainMemberId();
        assertEquals(
                 null,
                 actual2,
                "Successful format shoulda cleared assigned to storage domain member assignment.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getBucketId();
        assertEquals(
                null,
                actual1,
                "Successful format shoulda cleared assigned to bucket flag." );
        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getLastCheckpoint();
        assertEquals(
                "blank",
               actual,
                "Successful format shoulda updated last checkpoint.");
    }
    
    
    @Test
    public void testRunWithoutTapeDensityDirectivePassesDownNullForDensityDesired()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO5 );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        mockDaoDriver.createTapeDensityDirective(
                mockDaoDriver.createTapePartition( null, null ).getId(), tape.getType(), TapeDriveType.LTO6 );
        mockDaoDriver.createTapeDensityDirective(
                tape.getPartitionId(), TapeType.LTO7, TapeDriveType.LTO6 );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );
        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_IN_PROGRESS,
                 actual,
                "Shoulda updated state to in progress.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(
                null,
                 tapeDriveResource.getFormatCalls().get(0),
                "Shoulda made format request.");
    }
    
    
    @Test
    public void testRunWithTapeDensityDirectivePassesDownNonNullForDensityDesired()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape = mockDaoDriver.createTape( null, TapeState.NORMAL, TapeType.LTO5 );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        mockDaoDriver.createTapeDensityDirective(
                mockDaoDriver.createTapePartition( null, null ).getId(), tape.getType(), TapeDriveType.LTO5 );
        mockDaoDriver.createTapeDensityDirective(
                tape.getPartitionId(), tape.getType(), TapeDriveType.LTO6 );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );
        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_IN_PROGRESS,
                 actual,
                "Shoulda updated state to in progress.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(
                TapeDriveType.LTO6,
                 tapeDriveResource.getFormatCalls().get(0),
                "Shoulda made format request.");
    }
    
    
    @Test
    public void testRunWithoutErrorsWhenTapeWasNotAssignedToStorageDomainReclaimsGenerally()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
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
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( null )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isAssignedToStorageDomain();
        assertEquals(
                false,
                actual2,
                "Successful format shoulda cleared assigned to bucket flag.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getStorageDomainMemberId();
        assertEquals(
                 null,
                actual1,
                "Successful format shoulda cleared assigned to storage domain member assignment.");
        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getBucketId();
        assertEquals(
                 null,
                 actual,
                "Successful format shoulda cleared assigned to bucket flag.");
    }
    
    
    @Test
    public void testRunWithoutErrorsWhenTapeWasAssignedToStorageDomainAndBucketNonSecurelyReclaimsGenerally()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
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
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isAssignedToStorageDomain();
        assertEquals(
                 false,
                actual2,
                "Successful format shoulda cleared assigned to bucket flag.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getStorageDomainMemberId();
        assertEquals(
                 null,
                 actual1,
                "Successful format shoulda cleared assigned to storage domain member assignment.");
        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).getBucketId();
        assertEquals(
                 null,
                 actual,
                "Successful format shoulda cleared assigned to bucket flag.");
    }
    
    
    @Test
    public void testRunWithoutErrorsWhenTapeWasAssignedToStorageDomainAndBucketSecurelyReclaimsToBucket()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
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
        
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() )
                .setBucketId( bucket.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );
        
        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isAssignedToStorageDomain();
        assertEquals(
                 true,
                actual,
                "Secure bucket isolation mode shoulda been respected.");
        final Object expected1 = sdm.getId();
        final Object expected3 =  dbSupport.getServiceManager().getRetriever(Tape.class).attain(
                tape.getId()).getStorageDomainMemberId();
        assertEquals(
                expected1,
                expected3,
                "Secure bucket isolation mode shoulda been respected.");
        final Object expected = bucket.getId();
        final Object expected2 =  dbSupport.getServiceManager().getRetriever(Tape.class).attain(
                tape.getId()).getBucketId();
        assertEquals(
                 expected,
                expected2,
                "Secure bucket isolation mode shoulda been respected.");
    }


    @Test
    public void testRunWithoutErrorsWhenTapeWasAssignedToStorageDomainSecurelyReclaimsToStorageDomain()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
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
        mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        
        final Tape tape = mockDaoDriver.createTape();
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final UUID tapeDriveId = mockDaoDriver.createTapeDrive( null, "a", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.values()[ 0 ], new RuntimeException("oops"));
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDriveId ) );

        TestUtil.invokeAndWaitUnchecked( task );

        final Object actual = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() ).isAssignedToStorageDomain();
        assertEquals(
                 true,
                 actual,
                "Secure bucket isolation mode shoulda been respected.");
        final Object expected = sdm.getId();
        final Object expected1 =  dbSupport.getServiceManager().getRetriever(Tape.class).attain(
                tape.getId()).getStorageDomainMemberId();
        assertEquals(
                 expected1,
                 expected,
                "Secure bucket isolation mode shoulda been respected.");
        assertNull(
                dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                        tape.getId() ).getBucketId(),
                "Standard bucket isolation mode shoulda been respected."
                 );
    }


    @Test
    public void testFormatTapeTaskIsMarkedCompleteWhenTapeMarkedBad()
    {
        
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());

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
        mockDaoDriver.createBucket( null, dp.getId(), "bucket" );

        final Tape tape = mockDaoDriver.createTape(TapeState.FORMAT_PENDING);

        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape.getPartitionId(), tape.getType() );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape.setFullOfData( true ).setLastCheckpoint( "blah" )
                        .setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() ),
                Tape.FULL_OF_DATA, Tape.LAST_CHECKPOINT,
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final TapeDrive tapeDrive1 = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        // Register previous format failures across two drives
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.FORMAT_FAILED, new RuntimeException("oops"));
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.FORMAT_FAILED, new RuntimeException("oops"));

        dbSupport.getServiceManager().getService(TapeDriveService.class).update(tapeDrive1.setTapeId(null), TapeDrive.TAPE_ID);
        final UUID tapeDriveId2 = mockDaoDriver.createTapeDrive( null, "b", tape.getId() ).getId();
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.FORMAT_FAILED, new RuntimeException("oops"));

        // Set up the tape drive resource so the format will fail, then run the task
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailFormat(true);
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );

        try {
            task.prepareForExecutionIfPossible(tapeDriveResource, new MockTapeAvailability().setDriveId(tapeDriveId2));
            TestUtil.invokeAndWaitUnchecked(task);
        } catch ( final Exception e ) {
            Assertions.fail("Task should have completed, not thrown an exception " + e.getMessage());
        }

        // Verify the tape was marked bad and the task completed
        final Tape dbTape = dbSupport.getServiceManager().getRetriever( Tape.class ).attain(
                tape.getId() );

        final Object actual1 = dbTape.getState();
        assertEquals(
                 TapeState.BAD,
                 actual1,
                "Expected tape to be marked bad with 3 failed formats");

        final Object actual = task.getState();
        assertEquals(
                 BlobStoreTaskState.COMPLETED,
                 actual,
                "Expected task to be marked COMPLETED.");

    }
    
    
    @Test
    public void testRunAgainstTapeWithForeignOwnershipProceedsAnyway()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object actual2 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                TapeState.FORMAT_IN_PROGRESS,
                 actual2,
                "Shoulda updated state to in progress.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(
                 true,
                tapeDriveResource.isFormatInvoked(),
                "Shoulda made format request.");
        final Object expected = tape.getId();
        final Object expected1 =  tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId();
        assertEquals(
                 expected,
                 expected1,
                "Shoulda updated tape ownership.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.NORMAL,
                 actual1,
                "Shoulda updated state to normal.");
        final Object actual = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
                0,
                actual,
                "Should notta recorded failure.");
    }
    
    
    @Test
    public void testRunWithFormatFailureUpdatesTapeStateAndMakesAppropriateCallsToTapeDriveResource()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.putBlobOnTape( tape.getId(), mockDaoDriver.getBlobFor( o.getId() ).getId() );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        final Object actual4 = tapeService.attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_IN_PROGRESS,
                 actual4,
                "Shoulda updated state to in progress.");
        tapeDriveResource.setFailFormat( true );
        TestUtil.assertThrows( null, RpcProxyException.class, () -> TestUtil.invokeAndWaitChecked( task ) );
        assertEquals(
                 true,
                tapeDriveResource.isFormatInvoked(),
                "Shoulda made format request." );
        final Object actual3 = tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId();
        assertEquals(
                 null,
                 actual3,
                "Should notta updated tape ownership." );
        final Object actual2 = tapeService.attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_PENDING,
                actual2,
                "Shoulda updated state to format pending due to failure.");
        final Object actual1 = tapeService.attain( tape.getId() ).getLastCheckpoint();
        assertEquals(
                null,
                actual1,
                "Should notta updated checkpoint." );
        final Object actual = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
                1,
                actual,
                "Shoulda recorded failure.");
    }
    
    @Test
    public void testRunWhenBlobsOnTapeWithFormatFailure()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.putBlobOnTape( tape.getId(), mockDaoDriver.getBlobFor( o.getId() ).getId() );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( 
                tape, TapeState.FORMAT_PENDING );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final FormatTapeTask task = new FormatTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), tapeFailureManagement, dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        final Object actual6 = tapeService.attain( tape.getId() ).getState();
        assertEquals(
                 TapeState.FORMAT_IN_PROGRESS,
                actual6,
                "Shoulda updated state to in progress.");
        tapeDriveResource.setFailFormat( true );
        final Object actual5 = dbSupport.getServiceManager().getRetriever( BlobTape.class ).getCount();
        assertEquals(
                1,
                actual5,
                "Shoulda had blob records for tape.");
        TestUtil.assertThrows( null, RpcProxyException.class, () -> TestUtil.invokeAndWaitChecked( task ) );
        assertEquals(
                true,
                tapeDriveResource.isFormatInvoked(),
                "Shoulda made format request.");
        final Object actual4 = tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId();
        assertEquals(
                null,
                actual4,
                "Should notta updated tape ownership.");
        final Object actual3 = tapeService.attain( tape.getId() ).getState();
        assertEquals(
                TapeState.FORMAT_PENDING,
                actual3,
                "Shoulda updated state to format pending due to failure.");
        final Object actual2 = tapeService.attain( tape.getId() ).getLastCheckpoint();
        assertEquals(
                 null,
                actual2,
                "Should notta updated checkpoint.");
        final Object actual1 = dbSupport.getServiceManager().getRetriever( TapeFailure.class ).getCount();
        assertEquals(
               1,
                actual1,
                "Shoulda recorded failure.");
        final Object actual = dbSupport.getServiceManager().getRetriever( BlobTape.class ).getCount();
        assertEquals(
                0,
                actual,
                "Shoulda whacked blob records for tape.");
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
