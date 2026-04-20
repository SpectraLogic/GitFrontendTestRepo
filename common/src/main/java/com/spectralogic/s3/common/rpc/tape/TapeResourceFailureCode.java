/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape;

import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;

/**
 * Failure codes that may be propagated up by any implementor of any of the tape RPC resources that a client
 * may need to know about and/or check for.
 */
public enum TapeResourceFailureCode
{
    /**
     * Applies to {@link TapePartitionResource}.  <br><br>
     * 
     * A unit attention occurred when making the request.  The client should re-discover the tape environment
     * and then re-issue any commands as necessary.  Note: If this failure code occurs, the request did not 
     * execute partially or completely (no changes will have been made).
     */
    TAPE_ENVIRONMENT_CHANGED,
    
    
    /**
     * Applies to {@link TapeDriveResource#verifyQuiescedToCheckpoint}.
     */
    CHECKPOINT_NOT_FOUND,

    /**
     * Applies to {@link TapeDriveResource#verifyQuiescedToCheckpoint}.
     */
    CHECKPOINT_DATA_LOSS,


    /**
     * Applies to {@link TapeDriveResource#verifyQuiescedToCheckpoint}.
     */
    CHECKPOINT_ROLLBACK_TOO_FAR,
    
    
    /**
     * Applies to any RPC request made on {@link TapeDriveResource}.  If this error code comes back, it means
     * that the drive is not in a proper state to perform the requested operation and the failure should not
     * be considered a hard failure in the sense that a retry later could succeed.  <br><br>
     * 
     * In order to get the drive out of this state, the tape must be removed from the drive, which will be
     * indicated via {@link TapeDriveInformation#FORCE_TAPE_REMOVAL}.
     */
    BAD_DRIVE_STATE,
    
    
    /**
     * The resource is busy executing another command and does not support concurrent command execution.  Come
     * back later to issue your request.  Note: If this failure code occurs, the request did not execute 
     * partially or completely (no changes will have been made).
     */
    CONCURRENT_EXECUTION_NOT_SUPPORTED,


    /**
     * There was an encryption error. The drive was likely unable to retrieve the appropriate key from the LCM,
     * possibly due to a connection error.
     */
    ENCRYPTION_ERROR,


    /**
     * A hardware error was returned, suggesting there is a problem definitely caused by the drive and not the tape
     */
    HARDWARE_ERROR,

    /**
     * An LTFS error was returned, suggesting there may be something wrong with the tape
     */
    LTFS_ERROR
}
