package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapePartitionLockSupport;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;

import java.util.UUID;

public interface TapeEnvironment extends TapeEnvironmentManager {
    boolean ensurePhysicalTapeEnvironmentUpToDate(final Refresh refresh);

    void flagForRefresh();

    void flagForQuiesceStateAndRefresh();

    void performMove(
            final TapeDriveResource tapeDriveResource,
            final String partitionSerial,
            final Tape tape,
            final TapeMoveStrategy moveStrategy,
            final boolean requireSuccessfulPrepareForRemovalToProceedWithMove );


    boolean moveTapeToSlot(
            final UUID tapeId,
            final ElementAddressType destinationSlotType,
            final TapeMoveListener listener );


    Object getTapeLockHolder( final UUID tapeId );


    boolean tryPartitionLock( final String partitionSerial );


    void lockPartition( final String partitionSerial );


    void unlockPartition( final String partitionSerial );


    boolean threadsWaitingForPartitionLock(final String partitionSerial);


    boolean isTaskExecutionSuspended(final UUID partitionId);


    void deleteOfflineTapePartition( final UUID partitionId );


    void deleteOfflineTapeDrive( final UUID tapeDriveId );


    void deletePermanentlyLostTape( final UUID tapeId );


    void addSchedulingListener( final BlobStoreTaskSchedulingListener listener );


    final static String MISSING_BAR_CODE_PREFIX = "MISSING-";
    final static String CHANGED_BAR_CODE_PREFIX = "CHANGED-";
}
