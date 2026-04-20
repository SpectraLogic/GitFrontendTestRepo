package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TapeFailureManagement_Test  {


    private void assertFalseMod(final String message,  final boolean actual) {
        assertFalse(actual,  message);
    }

    private void assertNullMod(final String message,  final Object actual) {
        assertNull(actual,  message);
    }

    private void assertNotNullMod(final String message,  final Object actual) {
        assertNotNull(actual,  message);
    }


    @Test
    public void testMultipleReadFailuresOnSameTape() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive(null, null , tape.getId());
        final TapeDrive otherDrive = mockDaoDriver.createTapeDrive(null, null);

        final TapeFailureService tapeFailureService = dbSupport.getServiceManager().getService(TapeFailureService.class);

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());

        final Object expected5 = tapeFailureService.getCount();
        assertEquals(expected5, 0);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(tapeDrive.getId()).size(),0);

        // Register a failure and verify drive is not marked as having too many attempts
        final Throwable throwable = new Throwable("Test throwable");
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.BLOB_READ_FAILED, throwable);
        final Object expected4 = tapeFailureService.getCount();
        assertEquals(expected4, 1);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(tapeDrive.getId()).size(), 0);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(otherDrive.getId()).size(), 0);

        // Register a second failure and verify the drive is marked as having too many attempts
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.BLOB_READ_FAILED, throwable);
        final Object expected3 = tapeFailureService.getCount();
        assertEquals(expected3, 2);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(tapeDrive.getId()).size(),  1);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(otherDrive.getId()).size(), 0);

        // Verify resetting specific failures
        tapeFailureManagement.resetFailures(tape.getId(), tapeDrive.getId(), TapeFailureType.WRITE_FAILED);
        final Object expected2 = tapeFailureService.getCount();
        assertEquals(expected2, 2);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(tapeDrive.getId()).size(), 1);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(otherDrive.getId()).size(),  0);

        tapeFailureManagement.resetFailures(tape.getId(), tapeDrive.getId(), TapeFailureType.BLOB_READ_FAILED);
        final Object expected1 = tapeFailureService.getCount();
        assertEquals(expected1, 2);
        assertEquals(tapeFailureManagement.getTapesAttemptedTooManyTimesOnDrive(tapeDrive.getId()).size(), 0);

        // Verify the tape drive was not quiesced
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Object expected = tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced();
        assertEquals(expected, Quiesced.NO);
    }

    @Test
    public void testSetTapeBadOnWriteFailedMultipleDrives() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final Tape tape = mockDaoDriver.createTape(TapeState.NORMAL);

        final List<TapeDrive> drives = Arrays.asList(
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null)
        );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Throwable throwable = new Throwable("Test throwable");
        for (final TapeDrive drive : drives) {
            final Object expected1 = tapeDriveService.retrieve(drive.getId()).getQuiesced();
            assertEquals(expected1, Quiesced.NO);
            final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
            assertEquals(expected, TapeState.NORMAL);
            mockDaoDriver.updateBean(drive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.WRITE_FAILED, throwable);
            mockDaoDriver.updateBean(drive.setTapeId(null), TapeDrive.TAPE_ID);
        }
        final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
        assertEquals(expected, TapeState.BAD);
    }

    @Test
    public void testSetTapeFullOnWriteFailedMultipleDrives() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        // Create a tape that already has has data
        final Tape tape = mockDaoDriver.createTape(TapeState.NORMAL);

        final Bucket bucket = mockDaoDriver.createBucket( null, "testBucket" );
        final S3Object object = mockDaoDriver.createObject( bucket.getId(), "testObject" );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        mockDaoDriver.putBlobOnTape(tape.getId(), blob.getId());

        final List<TapeDrive> drives = Arrays.asList(
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null)
        );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Throwable throwable = new Throwable("Test throwable");
        for (final TapeDrive drive : drives) {
            final Object expected1 = tapeDriveService.retrieve(drive.getId()).getQuiesced();
            assertEquals( expected1, Quiesced.NO );
            final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
            assertEquals(expected, TapeState.NORMAL);
            assertFalse(mockDaoDriver.retrieve(Tape.class, tape.getId()).isFullOfData());
            mockDaoDriver.updateBean(drive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.WRITE_FAILED, throwable);
            mockDaoDriver.updateBean(drive.setTapeId(null), TapeDrive.TAPE_ID);
        }
        final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
        assertEquals(TapeState.NORMAL, mockDaoDriver.retrieve(Tape.class, tape.getId()).getState());
        final String message = String.valueOf(mockDaoDriver.retrieve(Tape.class, tape.getId()).isFullOfData());
        assertTrue(mockDaoDriver.retrieve(Tape.class, tape.getId()).isFullOfData());
    }

    @Test
    public void testQuiesceSuspectedDrive() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final TapeFailureService tapeFailureService = dbSupport.getServiceManager().getService(TapeFailureService.class);

        final List<Tape> tapes = Arrays.asList(
                mockDaoDriver.createTape(),
                mockDaoDriver.createTape(),
                mockDaoDriver.createTape());
        final Tape tape1 = tapes.get(0);


        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive(null, null);

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Throwable throwable = new Throwable("Test throwable");
        for (final Tape tape : tapes) {
            final Object expected = tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced();
            assertEquals(expected, Quiesced.NO);
            mockDaoDriver.updateBean(tapeDrive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.BLOB_READ_FAILED, throwable);
        }
        assertEquals(Quiesced.PENDING, tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced(), "Should have quiesced drive.");
        assertEquals(3,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.BLOB_READ_FAILED)), "Should have been 3 read failures");
        assertEquals(1,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.QUIESCING_DRIVE)), "Should have been 1 quiesced drive failure to notify user");
        assertEquals(4,  tapeFailureService.getCount(), "Should have been 4 failures total");

        mockDaoDriver.updateBean(tapeDrive.setQuiesced(Quiesced.NO).setTapeId(tape1.getId()), TapeDrive.QUIESCED, TapeDrive.TAPE_ID);
        tapeFailureManagement.registerFailure(tape1.getId(), TapeFailureType.BLOB_READ_FAILED, throwable);

        assertEquals(4,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.BLOB_READ_FAILED)), "Should have been 4 read failures");
        assertEquals(1,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.QUIESCING_DRIVE)), "Should still only be 1 quiesce failure due to reset");

        for (final Tape tape : tapes) {
            final Object expected = tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced();
            assertEquals(expected, Quiesced.NO);
            mockDaoDriver.updateBean(tapeDrive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.BLOB_READ_FAILED, throwable);
        }

        assertEquals(Quiesced.PENDING, tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced(), "Should have quiesced again.");

        assertEquals(7,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.BLOB_READ_FAILED)), "Should have been 7 read failures");
        assertEquals(2,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.QUIESCING_DRIVE)), "Should have been 2 quiesced drive failure to notify user");
        assertEquals(9,  tapeFailureService.getCount(), "Should have been 9 failures total");
    }

    @Test
    public void testRegisteringDriveCleanEvent() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive(null, null, tape.getId());

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());

        final TapeFailureService tapeFailureService = dbSupport.getServiceManager().getService(TapeFailureService.class);

        // Register a drive clean failed
        final Throwable throwable = new Throwable("Test throwable");
        tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.DRIVE_CLEAN_FAILED, throwable);

        final List<TapeFailure> cleanFailedFailures = tapeFailureService.retrieveAll(
                Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.DRIVE_CLEAN_FAILED)).toList();
        assertEquals(cleanFailedFailures.size(), 1);

        // Verify the drive cleaning success message is sent when the drive clean failures are reset
        tapeFailureManagement.resetFailures(tape.getId(), tapeDrive.getId(), TapeFailureType.DRIVE_CLEAN_FAILED);

        final List<TapeFailure> cleanSucceededFailures = tapeFailureService.retrieveAll(
                Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.DRIVE_CLEANED)).toList();
        assertEquals(cleanSucceededFailures.size(), 1);
    }

    @Test
    public void testIncompatibleFailureTypeDoesNotCountAgainstDrive() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);
        final TapeFailureService tapeFailureService = dbSupport.getServiceManager().getService(TapeFailureService.class);

        final List<Tape> tapes = Arrays.asList(
                mockDaoDriver.createTape(),
                mockDaoDriver.createTape(),
                mockDaoDriver.createTape());

        final TapeDrive tapeDrive = mockDaoDriver.createTapeDrive(null, null);

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Throwable throwable = new Throwable("Test throwable");
        for (final Tape tape : tapes) {
            final Object expected = tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced();
            assertEquals(expected, Quiesced.NO);
            mockDaoDriver.updateBean(tapeDrive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.INCOMPATIBLE, throwable);
        }
        assertEquals(Quiesced.NO, tapeDriveService.retrieve(tapeDrive.getId()).getQuiesced(), "Should not have quiesced drive.");
        assertEquals(3,  tapeFailureService.getCount(Require.beanPropertyEquals(TapeFailure.TYPE, TapeFailureType.INCOMPATIBLE)), "Should have been 3 incompatible failures");
    }

    @Test
    public void testSetTapeBadOnFormatFailedMultipleDrives() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final Tape tape = mockDaoDriver.createTape(TapeState.NORMAL);

        final List<TapeDrive> drives = Arrays.asList(
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null)
        );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Throwable throwable = new Throwable("Test throwable");
        for (final TapeDrive drive : drives) {
            final Object expected1 = tapeDriveService.retrieve(drive.getId()).getQuiesced();
            assertEquals(expected1, Quiesced.NO);
            final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
            assertEquals(expected, TapeState.NORMAL);
            mockDaoDriver.updateBean(drive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.FORMAT_FAILED, throwable);
            mockDaoDriver.updateBean(drive.setTapeId(null), TapeDrive.TAPE_ID);
        }
        final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
        assertEquals(expected, TapeState.BAD);
    }

    @Test
    public void testSetTapeBadOnSinglePartition() {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

        final MockDaoDriver mockDaoDriver = new MockDaoDriver(dbSupport);

        final Tape tape = mockDaoDriver.createTape(TapeState.NORMAL);

        final List<TapeDrive> drives = Arrays.asList(
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null),
                mockDaoDriver.createTapeDrive(null, null)
        );

        final TapeFailureManagement tapeFailureManagement = new TapeFailureManagement(dbSupport.getServiceManager());
        final TapeDriveService tapeDriveService = dbSupport.getServiceManager().getService(TapeDriveService.class);
        final Throwable throwable = new Throwable("Test throwable");
        for (final TapeDrive drive : drives) {
            final Object expected2 = tapeDriveService.retrieve(drive.getId()).getQuiesced();
            assertEquals(expected2, Quiesced.NO);
            final Object expected1 = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
            assertEquals(expected1, TapeState.NORMAL);
            mockDaoDriver.updateBean(drive.setTapeId(tape.getId()), TapeDrive.TAPE_ID);
            tapeFailureManagement.registerFailure(tape.getId(), TapeFailureType.SINGLE_PARTITION, throwable);
            final Object expected = tapeDriveService.retrieve(drive.getId()).getQuiesced();
            assertEquals(expected, Quiesced.NO);
            mockDaoDriver.updateBean(drive.setTapeId(null), TapeDrive.TAPE_ID);
        }
        final Object expected = mockDaoDriver.retrieve(Tape.class, tape.getId()).getState();
        assertEquals(expected, TapeState.BAD);
    }
}
