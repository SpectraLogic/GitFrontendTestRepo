package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.shared.Severity;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.dataplanner.backend.tape.task.BlobFailuresOccurredException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TapeFailureManagement {

    public TapeFailureManagement(final TapeFailureService tapeFailureService,
                                 final TapeDriveService tapeDriveService,
                                 final TapeService tapeService) {
        m_tapeFailureService = tapeFailureService;
        m_tapeDriveService = tapeDriveService;
        m_tapeService = tapeService;
        m_failureExpirationExecutor.start();
    }

    public TapeFailureManagement(final BeansServiceManager beansServiceManager) {
        this(
                beansServiceManager.getService(TapeFailureService.class),
                beansServiceManager.getService(TapeDriveService.class),
                beansServiceManager.getService(TapeService.class)
        );

    }

    synchronized public TapeFailureAction registerFailure(final UUID tapeId, final TapeFailureType type, final Throwable t) {
        final TapeDrive tapeDrive = m_tapeDriveService.attain(TapeDrive.TAPE_ID, tapeId );
        final UUID tapeDriveId = tapeDrive.getId();
        return registerFailure(tapeId, tapeDriveId, type, t);
    }
    
    synchronized public TapeFailureAction registerFailure(final UUID tapeId, final UUID tapeDriveId, final TapeFailureType type, final Throwable t) {
        final TapeDrive tapeDrive = m_tapeDriveService.attain( tapeDriveId );
        final TapeFailure failure = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage(ExceptionUtil.getReadableMessage(t))
                .setTapeId(tapeId)
                .setTapeDriveId(tapeDriveId)
                .setType(type);
        m_tapeFailureService.create(failure);
        if (type.getSeverity() != Severity.SUCCESS && type.getSeverity() != Severity.INFO && type != TapeFailureType.QUIESCING_DRIVE) {
            //TODO: it would probably be good to add a separate failure message here specifically for announcing that the drive failed enough to be quiesced
            LOG.error(type.name() + " failure occurred for tape " + tapeId + " on drive " + tapeDriveId + ".", t);
            if (canBeBlamedOnDrive(failure.getType())) {
                m_failuresPotentiallyCausedByDrive.put(tapeDriveId, failure);
                final Set<UUID> failedTapes = tapesWithOutstandingFailureOnDrive(tapeDriveId);
                //If we have failures from 3 separate tapes, suspect the drive
                final boolean problemSuspectedOnDrive = tapeDrive.getMaxFailedTapes() != null
                        && failedTapes.size() >= tapeDrive.getMaxFailedTapes();
                if (problemSuspectedOnDrive) {
                    final TapeDrive drive = m_tapeDriveService.retrieve(tapeDriveId);
                    final String message = quiesceMessage(drive, failedTapes);
                    LOG.warn(message);
                    registerFailure(tapeId, TapeFailureType.QUIESCING_DRIVE, new RuntimeException(message));
                    m_tapeDriveService.update(drive.setQuiesced(Quiesced.PENDING), TapeDrive.QUIESCED);
                    //NOTE: we clear failures so they are not counted against drive again when it is unquiesced
                    m_failuresPotentiallyCausedByDrive.get(tapeDriveId).clear();
                    return TapeFailureAction.DRIVE_QUIESCED;
                }
            }

            if (canBeBlamedOnTape(failure.getType())) {
                m_failuresPotentiallyCausedByTape.put(tapeId, failure);
                if (problemSuspectedOnTape(tapeId, t)) {
                    markTapeAsBeingFullOrBad(tapeId);
                    return TapeFailureAction.TAPE_MARKED_BAD;
                }
            }
        }
        return TapeFailureAction.NONE;
    }

    private String quiesceMessage(final TapeDrive drive, final Set<UUID> failedTapes) {
        return "Will quiesce drive " + drive.getSerialNumber() + " due to recent failures on multiple tapes: " +
                m_tapeService.retrieveAll(failedTapes)
                .toSet()
                .stream()
                .map( (tape) -> tape.getBarCode() )
                .collect(Collectors.joining(", "));
    }

    private void markTapeAsBeingFullOrBad(final UUID tapeId)
    {
        final Tape tape = m_tapeService.attain(tapeId);
        LOG.warn( "We have given up attempting to write to " + tape.getBarCode()
                + ".  This tape appears to be bad." );
        if ( !m_tapeService.any( Require.all(
                Require.beanPropertyEquals( Identifiable.ID, tapeId ),
                Require.exists( BlobTape.class, BlobTape.TAPE_ID, Require.nothing() ) ) ) )
        {
            m_tapeService.transistState( tape, TapeState.BAD );
        }
        else
        {
            m_tapeService.update(
                    m_tapeService.attain( tapeId ).setFullOfData( true ),
                    Tape.FULL_OF_DATA );
        }
    }


    private Set<UUID> tapesWithOutstandingFailureOnDrive(final UUID tapeDriveId) {
        return m_failuresPotentiallyCausedByDrive.get(tapeDriveId)
                .stream()
                .map(TapeFailure::getTapeId)
                .collect(Collectors.toSet());
    }

    private boolean problemSuspectedOnTape(final UUID tapeId, final Throwable t) {
        if ( ( t instanceof BlobFailuresOccurredException ) && !( (BlobFailuresOccurredException)t ).isRetryable() ) {
            return true; //not retryable (out of space)
        }
        final Map<UUID, List<TapeFailure>> failuresByDrive = m_failuresPotentiallyCausedByTape.get(tapeId)
                .stream()
                .collect(Collectors.groupingBy((failure) -> failure.getTapeDriveId()));
        if (failuresByDrive.keySet().size() <= 1) {
            return false;
        }
        final boolean failedOnThreeOrMoreDrives = failuresByDrive.keySet().size() >= 3;
        final boolean failedOnBothDrivesTwiceEach =
                failuresByDrive.values().stream().allMatch( (failuresForDrive) -> failuresForDrive.size() >= 2);
        return failedOnThreeOrMoreDrives || failedOnBothDrivesTwiceEach;
    }

    synchronized public Set<UUID> getTapesAttemptedTooManyTimesOnDrive(final UUID tapeDriveId) {
        final Set<UUID> retval = new HashSet<>();
        final Set<UUID> failedOnce = new HashSet<>();
        for (final TapeFailure failure : m_failuresPotentiallyCausedByDrive.get(tapeDriveId)) {
            if (failedOnce.contains(failure.getTapeId())) {
                retval.add(failure.getTapeId());
            } else {
                failedOnce.add(failure.getTapeId());
            }
        }
        return retval;
    }

    //NOTE: any failure type that we actively store in this class should be reset when the operation is successful.
    //This does not clear any records in the database.
    synchronized public void resetFailures(final UUID tapeId, final UUID tapeDriveId, final TapeFailureType ... types) {
        if (Arrays.stream(types).anyMatch(type -> type == TapeFailureType.DRIVE_CLEAN_FAILED)) {
            registerDriveCleanedEvent(tapeId, tapeDriveId);
        }
        if (Arrays.stream(types).anyMatch(type -> type == TapeFailureType.DRIVE_TEST_FAILED)) {
            registerDriveTestedEvent(tapeId, tapeDriveId);
        }
        final Predicate<TapeFailure> matchesOneOfTypes = failure -> {
            return Arrays.stream(types).anyMatch(type -> type == failure.getType());
        };
        if (m_failuresPotentiallyCausedByTape.get(tapeId).removeIf(matchesOneOfTypes)) {
            for (final TapeFailureType type : types) {
                LOG.info("Cleared failures of type " + type + " for tape " + tapeId + " due to successful operation.");
            }
        }
        if (m_failuresPotentiallyCausedByDrive.get(tapeDriveId).removeIf(matchesOneOfTypes)) {
            for (final TapeFailureType type : types) {
                LOG.info("Cleared failures of type " + type + " for tape drive " + tapeDriveId + " due to successful operation.");
            }
        }
    }

    synchronized public void resetBlobReadFailuresWhenBlobMarkedSuspect(final UUID tapeId) {
        resetFailuresWhenBlobMarkedSuspect(tapeId, TapeFailureType.BLOB_READ_FAILED);
    }

    synchronized public void resetVerifyFailuresWhenBlobMarkedSuspect(final UUID tapeId) {
        resetFailuresWhenBlobMarkedSuspect(tapeId, TapeFailureType.VERIFY_FAILED);
    }

    private void resetFailuresWhenBlobMarkedSuspect(final UUID tapeId, final TapeFailureType tapeFailureType) {
        final Set<UUID> drivesCleared = new HashSet<>();
        final Predicate<Map.Entry<UUID, TapeFailure>> matchesBlobReadFromTape = failureEntry -> {
            final boolean shouldRemove = failureEntry.getValue().getType() == tapeFailureType
                    && failureEntry.getValue().getTapeId() == tapeId;
            if (shouldRemove) {
                drivesCleared.add(failureEntry.getKey());
            }
            return shouldRemove;
        };
        m_failuresPotentiallyCausedByDrive.entries().removeIf(matchesBlobReadFromTape);

        for (final UUID driveId : drivesCleared) {
            LOG.info("Cleared failures of type " + tapeFailureType + " for tape drive " + driveId
                    + " when reading from tape " + tapeId + " due to blob marked suspect.");
        }
    }

    private void registerDriveCleanedEvent(final UUID tapeId, final UUID tapeDriveId) {
        final TapeDrive drive = m_tapeDriveService.retrieve(tapeDriveId);
        final TapeFailure successEvent = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("The tape drive '" + drive.getSerialNumber() + "' was successfully cleaned.")
                .setTapeId(tapeId)
                .setTapeDriveId(tapeDriveId)
                .setType(TapeFailureType.DRIVE_CLEANED);
        m_tapeFailureService.create(successEvent);
    }


    private void registerDriveTestedEvent(final UUID tapeId, final UUID tapeDriveId) {
        final TapeDrive drive = m_tapeDriveService.retrieve(tapeDriveId);
        final TapeFailure successEvent = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("The tape drive '" + drive.getSerialNumber() + "' was successfully tested.")
                .setTapeId(tapeId)
                .setTapeDriveId(tapeDriveId)
                .setType(TapeFailureType.DRIVE_TEST_SUCCEEDED);
        m_tapeFailureService.create(successEvent);
    }


    private boolean canBeBlamedOnDrive(final TapeFailureType type) {
        return ImmutableList.of(
                TapeFailureType.BLOB_READ_FAILED,
                TapeFailureType.DATA_CHECKPOINT_FAILURE,
                TapeFailureType.DRIVE_CLEAN_FAILED,
                TapeFailureType.FORMAT_FAILED,
                TapeFailureType.GET_TAPE_INFORMATION_FAILED,
                TapeFailureType.MOVE_FAILED,
                TapeFailureType.IMPORT_FAILED,
                TapeFailureType.IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE,
                TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY,
                TapeFailureType.INSPECT_FAILED,
                TapeFailureType.READ_FAILED,
                TapeFailureType.VERIFY_FAILED,
                TapeFailureType.WRITE_FAILED,
                TapeFailureType.ENCRYPTION_ERROR,
                TapeFailureType.HARDWARE_ERROR,
                TapeFailureType.DELAYED_OWNERSHIP_FAILURE
        ).contains(type);
    }

    //NOTE: while many problems can be blamed on tape, we only tally write failures and format failures here. Other
    //failures are handled by their respective tasks, either by marking blobs suspect, or changing the tape's state.
    //Failures that can be blamed on tape will result in either a tape being marked bad if its empty, or being marked
    //full of data if not empty.
    private boolean canBeBlamedOnTape(final TapeFailureType type) {
        return ImmutableList.of(
                TapeFailureType.WRITE_FAILED,
                TapeFailureType.FORMAT_FAILED,
                TapeFailureType.DELAYED_OWNERSHIP_FAILURE,
                TapeFailureType.SINGLE_PARTITION
        ).contains(type);
    }

    synchronized private void ageOutOldFailures() {
        final long earliestAllowedDate = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(MAX_FAILURE_AGE_IN_HOURS);
        final boolean agedOutForTape = m_failuresPotentiallyCausedByTape.values().removeIf(failure -> failure.getDate().getTime() < earliestAllowedDate);
        final boolean agedOutForDrive = m_failuresPotentiallyCausedByDrive.values().removeIf(failure -> failure.getDate().getTime() < earliestAllowedDate);
        if (agedOutForTape || agedOutForDrive) {
            LOG.info("Aged out tape failures that occurred over " + MAX_FAILURE_AGE_IN_HOURS + " hours ago.");
        }
    }

    private final TapeFailureService m_tapeFailureService;
    private final TapeDriveService m_tapeDriveService;
    private final TapeService m_tapeService;
    private final Multimap<UUID, TapeFailure> m_failuresPotentiallyCausedByTape = MultimapBuilder.hashKeys().arrayListValues().build();
    private final Multimap<UUID, TapeFailure> m_failuresPotentiallyCausedByDrive = MultimapBuilder.hashKeys().arrayListValues().build();
    private final RecurringRunnableExecutor m_failureExpirationExecutor =
            new RecurringRunnableExecutor(this::ageOutOldFailures, TimeUnit.MINUTES.toMillis(5));
    private final static int MAX_FAILURE_AGE_IN_HOURS = 24;
    private final static Logger LOG = Logger.getLogger( TapeFailureManagement.class );


}