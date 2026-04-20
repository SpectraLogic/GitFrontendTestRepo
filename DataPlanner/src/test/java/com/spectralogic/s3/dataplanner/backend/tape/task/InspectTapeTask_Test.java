/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment.CHANGED_BAR_CODE_PREFIX;
import static org.junit.jupiter.api.Assertions.*;


public final class InspectTapeTask_Test 
{
    @Test
    public void testNonSpectraTapeTypeGetsCorrectedByRunningTask()
    {

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );
        
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeType.LTO5, tapeService.attain( tape.getId() ).getType(), "Shoulda corrected tape type.");
    }


   @Test
    public void testBarcodeChangeDetectedAndUpdated()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape1 );
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape2 );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, null, tape2.getId() );
        mockDaoDriver.updateBean( tape1.setBarCode( "bc1" ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tape2.setBarCode( "bc2" ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tape1.setSerialNumber( "abc" ), SerialNumberObservable.SERIAL_NUMBER );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeSerialNumber( "abc" );
        final InspectTapeTask task =
                new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape2.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        assertEquals(TapeState.PENDING_INSPECTION, mockDaoDriver.attain( tape1 ).getState(), "Should not have changed state");
        assertEquals(TapeState.PENDING_INSPECTION, mockDaoDriver.attain( tape2 ).getState(), "Should not have changed state");

        assertEquals("bc2", mockDaoDriver.attain( tape1 ).getBarCode(), "Shoulda failed out both tapes with sn mismatch.");
        assertEquals(CHANGED_BAR_CODE_PREFIX + "bc1", mockDaoDriver.attain( tape2 ).getBarCode(), "Shoulda failed out both tapes with sn mismatch.");

        final Object expected = tape2.getId();
        assertEquals(expected, mockDaoDriver.attain( drive ).getTapeId(), "Shoulda retained tape records as is.");
        assertEquals(TapeFailureType.BAR_CODE_CHANGED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda generated tape failure.");
    }


   @Test
    public void testSerialNumberMatchesThatForTapesWithSameBarcodeNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID libraryId = mockDaoDriver.createLibrary("sn1").getId();
        final TapePartition tapePartition = mockDaoDriver.createTapePartition(libraryId, "tp1");
        final TapePartition tapePartition2 = mockDaoDriver.createTapePartition(libraryId, "tp2");
        final Tape tape1 = mockDaoDriver.createTape(tapePartition.getId(), TapeState.EJECTED);
        mockDaoDriver.nullOutCapacityStats( tape1 );
        final Tape tape2 = mockDaoDriver.createTape(tapePartition2.getId(), TapeState.PENDING_INSPECTION);
        mockDaoDriver.nullOutCapacityStats( tape2 );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, null, tape2.getId() );
        mockDaoDriver.updateBean( tape1.setBarCode( "bc1" ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tape2.setBarCode( "bc1" ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tape1.setSerialNumber( "abc" ), SerialNumberObservable.SERIAL_NUMBER );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeSerialNumber( "abc" );
        final InspectTapeTask task =
                new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape2.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        assertEquals(TapeState.SERIAL_NUMBER_MISMATCH, mockDaoDriver.attain( tape1 ).getState(), "Shoulda failed out both tapes with sn mismatch.");
        assertEquals(TapeState.SERIAL_NUMBER_MISMATCH, mockDaoDriver.attain( tape2 ).getState(), "Shoulda failed out both tapes with sn mismatch.");

        assertEquals("bc1", mockDaoDriver.attain( tape1 ).getBarCode(), "Shoulda failed out both tapes with sn mismatch.");
        assertEquals("bc1", mockDaoDriver.attain( tape2 ).getBarCode(), "Shoulda failed out both tapes with sn mismatch.");

        final Object expected = tape2.getId();
        assertEquals(expected, mockDaoDriver.attain( drive ).getTapeId(), "Shoulda retained tape records as is.");
        assertEquals(TapeFailureType.SERIAL_NUMBER_MISMATCH, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda generated tape failure.");
    }


   @Test
    public void testSerialNumberChangesOnTapeNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape1 );
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape2 );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, null, tape1.getId() );
        mockDaoDriver.updateBean( tape1.setBarCode( "bc1" ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tape2.setBarCode( "bc2" ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tape1.setSerialNumber( "abc" ), SerialNumberObservable.SERIAL_NUMBER );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeSerialNumber( "def" );
        final InspectTapeTask task =
                new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape1.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.assertThrows( null, RuntimeException.class, () -> TestUtil.invokeAndWaitChecked( task ) );

        assertEquals(TapeState.SERIAL_NUMBER_MISMATCH, mockDaoDriver.attain( tape1 ).getState(), "Shoulda failed out tape with sn mismatch.");
        assertEquals(TapeState.PENDING_INSPECTION, mockDaoDriver.attain( tape2 ).getState(), "Shoulda failed out tape with sn mismatch.");

        assertEquals("bc1", mockDaoDriver.attain( tape1 ).getBarCode(), "Shoulda failed out tape with sn mismatch.");
        assertEquals("bc2", mockDaoDriver.attain( tape2 ).getBarCode(), "Shoulda failed out tape with sn mismatch.");

        final Object expected = tape1.getId();
        assertEquals(expected, mockDaoDriver.attain( drive ).getTapeId(), "Shoulda retained tape records as is.");
        assertEquals(TapeFailureType.SERIAL_NUMBER_MISMATCH, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda generated tape failure.");
    }


   @Test
    public void testWrongTapeTypeGetsCorrectedByRunningTask()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.LTO6 ), Tape.TYPE );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeType.LTO5, tapeService.attain( tape.getId() ).getType(), "Shoulda corrected tape type.");
    }


   @Test
    public void testIncorrectLto7TapeTypeGetsCorrectedToM8ByRunningTask()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId(), TapeDriveType.LTO7 );
        tapeService.update( tape.setType( TapeType.LTO7 ), Tape.TYPE );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        tapeDriveResource.setTapeType( TapeType.LTOM8 );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        final Tape result = tapeService.attain( tape.getId() );
        assertEquals(TapeType.LTOM8, result.getType(), "Should have corrected tape type.");
        assertEquals(TapeState.INCOMPATIBLE, result.getState(), "Should have marked tape as incompatible.");
    }


   @Test
    public void testInspectUnknownLtfsTapeResultsInLtfsForeignState()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setReturnNullOnInspect( Boolean.FALSE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.LTFS_WITH_FOREIGN_DATA, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to ltfs with foreign data.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectUnknownLtfsTapeInEjectPendingStateResultsInLtfsForeignState()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setReturnNullOnInspect( Boolean.FALSE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.LTFS_WITH_FOREIGN_DATA, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getPreviousState(), "Shoulda updated state to ltfs with foreign data.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should kept eject pending state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectCorruptTapeResultsInUnknownState()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailInspect( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.UNKNOWN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to unknown.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectBlankTapeResultsInFormat()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected1 = tape.getState();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(true, tapeDriveResource.isFormatInvoked(), "Shoulda made format request.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to normal.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
        assertEquals("blank", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda updated tape's checkpoint.");
    }


   @Test
    public void testInspectBlankTapeDoesNotResultInFormatForNonCompatibleMedia()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( null, null, TapeType.LTO5 );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive =
                mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId(), TapeDriveType.LTO7 );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager()
                                                                               .getRetriever( Tape.class )
                                                                               .attain( tape.getId() )
                                                                               .getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(TapeState.INCOMPATIBLE, dbSupport.getServiceManager()
                                                                                                 .getRetriever(
                                                                                                         Tape.class )
                                                                                                 .attain( tape.getId() )
                                                                                                 .getState(), "Shoulda updated state to incompatible.");
        assertEquals(1,  dbSupport.getServiceManager()
                .getRetriever(TapeFailure.class)
                .getCount(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectBlankTapeThatShouldHaveDataDoesntResultInFormat()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTapeBlobsFixture( "o1" );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(TapeState.BAD, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to bad.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeAlreadyInNormalStateResultsInTapeLeftAlone()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda left tape alone.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapePreviouslyInNormalStateResultsInTapeLeftAlone()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                .getPreviousState(), "Shoulda left tape alone.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda left tape alone.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeResultingInFormatThatFailsFormatWithExceptionUpdatesStateCorrectly()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailFormat( true );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(true, tapeDriveResource.isFormatInvoked(), "Shoulda made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.PENDING_INSPECTION, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to pending inspection.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectBlankTapeResultingInFormatThatFailsFormatDueToTapeReadOnlyUpdatesStateCorrectly()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.CANNOT_FORMAT_DUE_TO_WRITE_PROTECTION, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to cannot format due to write protection.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeWithInspectFailureResultsInStateUpdateWhenTapeUncheckpointedAndIsReadOnly()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setFailInspect( true );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.UNKNOWN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to unknown.");
        assertEquals(TapeFailureType.INSPECT_FAILED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectTapeWithInspectFailureResultsInStateUpdateWhenTapeHasCheckpointAndIsReadOnly()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        mockDaoDriver.updateBean( tape.setLastCheckpoint( "something" ), Tape.LAST_CHECKPOINT );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setFailInspect( true );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to unknown.");
        assertEquals(TapeFailureType.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectTapeWithInspectFailureResultsInStateUpdateWhenTapeUncheckpointedAndReadWrite()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailInspect( true );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.UNKNOWN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to unknown.");
        assertEquals(TapeFailureType.INSPECT_FAILED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectTapeWithInspectFailureResultsInStateUpdateWhenTapeHasCheckpointAndReadWrite()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        mockDaoDriver.updateBean( tape.setLastCheckpoint( "something" ), Tape.LAST_CHECKPOINT );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailInspect( true );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to unknown.");
        assertEquals(TapeFailureType.INSPECT_FAILED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectTapeWithLocalOwnershipResultsInNoFormatAndCorrectStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to normal.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeWithForeignOwnershipThatHasNotChangedSinceOwnedROResultsInCorrectStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        tape.setLastCheckpoint( "abc" );
        dbSupport.getServiceManager().getService( TapeService.class ).update( tape, Tape.LAST_CHECKPOINT );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setHasChangedSinceCheckpointResponse( false );
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());

        final MockTapeAvailability tapeAvailability = new MockTapeAvailability().setDriveId( tapeDrive.getId() );
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        final Object expected1 = tape.getState();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals("abc", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertTrue(dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .isTakeOwnershipPending(), "Shoulda noted take ownership pending.");
        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda retained state of normal.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");

        task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals("abc", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertTrue(dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .isTakeOwnershipPending(), "Shoulda noted take ownership pending.");
        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda retained state of normal.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");

        tapeDriveResource.setLoadedTapeReadOnly( false );
        task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals("blank", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda updated checkpoint.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .isTakeOwnershipPending(), "Shoulda noted no ownership pending.");
        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda retained state of normal.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeWithForeignOwnershipThatHasNotChangedSinceOwnedResultsInCorrectStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        tape.setLastCheckpoint( "abc" );
        dbSupport.getServiceManager().getService( TapeService.class ).update( tape, Tape.LAST_CHECKPOINT );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setHasChangedSinceCheckpointResponse( false );
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected1 = tape.getState();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals("blank", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda updated checkpoint.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .isTakeOwnershipPending(), "Should notta noted take ownership pending.");
        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to foreign.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeWithForeignOwnershipThatHasChangedSinceOwnedResultsInCorrectStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        tape.setLastCheckpoint( "abc" );
        dbSupport.getServiceManager().getService( TapeService.class ).update( tape, Tape.LAST_CHECKPOINT );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to foreign.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeWithForeignOwnershipThatFailsInspectionResultsInCorrectStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        tape.setLastCheckpoint( "abc" );
        dbSupport.getServiceManager().getService( TapeService.class ).update( tape, Tape.LAST_CHECKPOINT );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailInspect( true );
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to foreign.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectTapeWithForeignOwnershipResultsInNoFormatAndCorrectStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to foreign.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeAlreadyInNormalStateLeavesTapeAlone()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda left tape alone.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeWithFailureToUpdateTapeExtendedAttributesTransistsStateToPendingInspection()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.PENDING_INSPECTION, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda required another inspection.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure.");
    }


   @Test
    public void testInspectTapeImproperlyMarkedAsUnknownUpdatesToNormal()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda left tape alone.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeImproperlyMarkedAsUnknownUpdatesToForeign()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.UNKNOWN );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated tape to foreign state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeImproperlyMarkedAsForeignUpdatesToNormal()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda left tape alone.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeImproperlyMarkedAsNormalUpdatesToForeign()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated tape to foreign state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapePreviouslyInNormalStateLeavesTapeAlone()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda left tape alone.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                .getPreviousState(), "Shoulda left tape alone.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda left tape alone.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeInNormalStateThatIsOwnedByUsResultsInNoStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState( tape, TapeState.NORMAL );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda notta made format request.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeInNormalStateEjectPendingThatIsOwnedByUsResultsInNoStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.NORMAL );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected1 = tape.getState();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");
        final Object expected = tape.getPreviousState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getPreviousState(), "Should notta updated previous state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda notta made format request.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeInNormalPreviousStateThatIsOwnedByUsResultsInNoStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.NORMAL );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.PENDING_INSPECTION );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda notta made format request.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeNotInNormalStateThatIsOwnedByUsResultsInFormatAndStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected1 = tape.getState();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(true, tapeDriveResource.isFormatInvoked(), "Shoulda made format request.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeNotInNormalStateEjectPendingThatIsOwnedByUsResultsInDeferredInspection()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.nullOutCapacityStats( tape );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Shoulda deferred format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda deferred update tape ownership.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");
        assertEquals(TapeState.PENDING_INSPECTION, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getPreviousState(), "Shoulda updated previous state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeThatIsAlsoForeignResultsInFormatAndStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        tapeDriveResource.setReturnNullOnInspect( Boolean.TRUE );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource,
                new MockTapeAvailability( tapeDrive.getPartitionId(), tapeDrive.getId() ) );
        final Object expected1 = tape.getState();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(true, tapeDriveResource.isFormatInvoked(), "Shoulda made format request.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state to normal.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectBlankTapeThatIsEjectPendingResultsInNoFormatAndNoStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");
        assertEquals(TapeState.PENDING_INSPECTION, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getPreviousState(), "Shoulda updated previous state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectForeignTapeThatIsInconsistentResultsInNoFormatAndForeignStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        tapeDriveResource.setFailInspect( true );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda noted tape as foreign.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure for inspect as well as get formatted tape information.");
    }


   @Test
    public void testInspectForeignTapeThatIsEjectPendingResultsInNoFormatAndNoStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = UUID.randomUUID();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getPreviousState(), "Shoulda updated previous state.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta recorded failure.");
    }


   @Test
    public void testInspectTapeThatIsEjectPendingWithInspectFailureResultsInNoFormatAndNoStateChange()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.FORMAT_PENDING );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setFailInspect( true );
        final InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability().setDriveId( tapeDrive.getId() ) );
        final Object expected = tape.getState();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(null, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.FORMAT_PENDING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta updated state.");
        assertEquals(TapeState.UNKNOWN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getPreviousState(), "Shoulda updated previous state.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure.");
    }


   @Test
    public void testRawAvailableCapacityChangesToTapeHandledGracefully()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final UUID foreignOwnershipId = tape.getId();
        tapeDriveResource.setTapeId( foreignOwnershipId );
        tapeDriveResource.setFailGetFormattedTapeInformation( true );
        InspectTapeTask task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());

        final MockTapeAvailability tapeAvailability = new MockTapeAvailability().setDriveId( tapeDrive.getId() );
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.PENDING_INSPECTION, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state since inspection still required.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda recorded failure.");

        tapeDriveResource.setFailGetFormattedTapeInformation( false );
        task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(1000,  mockDaoDriver.attain(tape).getAvailableRawCapacity().intValue(), "Shoulda updated tape available raw capacity.");

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state since tape is normal.");

        tapeDriveResource.setGetFormattedTapeInformationAvailableRawCapacity( 700 );
        task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(700,  mockDaoDriver.attain(tape).getAvailableRawCapacity().intValue(), "Shoulda updated tape available raw capacity.");

        assertEquals(false, tapeDriveResource.isFormatInvoked(), "Should notta made format request.");
        assertEquals(foreignOwnershipId, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Should notta updated tape ownership.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda updated state since tape is normal.");

        tapeDriveResource.setGetFormattedTapeInformationAvailableRawCapacity( 800 );
        task = new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        TestUtil.invokeAndWaitUnchecked( task );
        assertEquals(800,  mockDaoDriver.attain(tape).getAvailableRawCapacity().intValue(), "Shoulda updated tape available raw capacity.");
    }

   @Test
    public void testMediumSinglePartitionError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setFailPartition( true );


        final InspectTapeTask task =
                new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        final MockTapeAvailability tapeAvailability = new MockTapeAvailability().setDriveId( tapeDrive.getId() );
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        task.runInternal();

        assertEquals(TapeFailureType.SINGLE_PARTITION, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda generated tape failure.");
    }

   @Test
    public void testInspectError()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.nullOutCapacityStats( tape );
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.NORMAL );
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive( tape.getPartitionId(), "a", tape.getId() );
        tapeService.update( tape.setType( TapeType.UNKNOWN ), Tape.TYPE );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setFailInspect( true );


        final InspectTapeTask task =
                new InspectTapeTask( BlobStoreTaskPriority.values()[ 0 ], tape.getId(), new TapeFailureManagement(dbSupport.getServiceManager()), dbSupport.getServiceManager());
        final MockTapeAvailability tapeAvailability = new MockTapeAvailability().setDriveId( tapeDrive.getId() );
        task.prepareForExecutionIfPossible( tapeDriveResource, tapeAvailability );
        task.runInternal();

        assertEquals(TapeFailureType.INSPECT_FAILED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda generated tape failure.");
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
