/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;

public interface TapeEnvironmentManager {
    
    /**
     * @return the slot number for the tape specified, or throws an exception if the tape id is unknown
     */
    int getTapeElementAddress( final UUID tapeId );


    /**
     * @return the slot number for the drive specified, or throws an exception if the drive id is unknown
     */
    int getDriveElementAddress( final UUID driveId );


    /**
     * @return TRUE if there is at least one slot available of the slot type specified in the partition
     * specified; FALSE otherwise
     */
    boolean isSlotAvailable( final UUID partitionId, final ElementAddressType slotType );


    /**
     * Finds a slot of the specified type that is available and selects it to move to.
     *
     * @return the slot the tape specified has been moved to, or throws an exception if there are no slots
     * available of the destination slot type specified.
     */
    int moveTapeSlotToSlot( final UUID tapeId, final ElementAddressType destinationSlotType );


    /**
     * Moves the specified tape to the specified slot.
     */
    void moveTapeSlotToSlot( final UUID tapeId, final int dest );


    /**
     * Moves the specified tape to the specified drive.
     */
    void moveTapeToDrive( final UUID tapeId, final TapeDrive drive );


    /**
     * Finds a slot of the specified type that is available and selects it to move to.
     *
     * @return the slot the tape specified has been moved to, or throws an exception if there are no slots
     * available of the destination slot type specified.
     */
    int moveTapeFromDrive( final TapeDrive drive, final ElementAddressType destinationSlotType );


    /**
     * Moves the tape in the src to the dest.  The dest must be empty and the src non-empty to make this call.
     */
    void moveTapeDriveToDrive( final TapeDrive src, final TapeDrive dest );


    /**
     * @return Set <tape id> for all tapes in the partition specified
     */
    Set< UUID > getTapesInPartition( final UUID partitionId );


    /**
     * @return Set <tape id> for all tapes not in the partition specified
     */
    Set< UUID > getTapesNotInPartition( final UUID partitionId );


    /**
     * @return Set <drive id> for all drives that require cleaning
     */
    Set< UUID > getDrivesRequiringCleaning();


    /**
     * @return Set <tape id> for all tapes that report being in a drive element address,
     * but no drive reports having that tape.
     */
    Set< UUID > getTapesInOfflineDrives();
}
