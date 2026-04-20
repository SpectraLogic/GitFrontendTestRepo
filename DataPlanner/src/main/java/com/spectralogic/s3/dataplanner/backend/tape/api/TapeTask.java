/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.api;

import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.api.RunnableBlobStoreTask;

import java.util.UUID;

/**
 * A task is a unit of work that operates on a single tape drive against a single tape (a task can only change
 * the tape in the tape drive assigned to it by asking to be re-executed).
 */
public interface TapeTask extends RunnableBlobStoreTask
{
    /**
     * @return the tape drive resource that this task uses
     */
    TapeDriveResource getDriveResource();
    
    
    /**
     * After this method is invoked, {@link #getTapeId()} should return non-null so that the correct tape can
     * be loaded into the tape drive before {@link Runnable#run()} is called.  If {@link #getTapeId()} returns
     * null, the task cannot be executed and should be retried later.
     */
    void prepareForExecutionIfPossible(
            final TapeDriveResource tapeDriveResource,
            final TapeAvailability tapeAvailability );


    /**
     * Checks if we can use the tape in the drive as is.
     * @return whether we can use the tape currently in the drive if there is one
     */
    boolean canUseTapeAlreadyInDrive(final TapeAvailability tapeAvailability );


    /**
     * Checks if we are allowed to use this drive.
     * @return true unless overwritten by implementing class to return false under special conditions.
     */
    default boolean canUseDrive(final UUID tapeDriveId ) {
        return true;
    }

    /**
     * Returns: whether we can use a tape that is currently available
     */
    boolean canUseAvailableTape(final TapeAvailability tapeAvailability);
}
