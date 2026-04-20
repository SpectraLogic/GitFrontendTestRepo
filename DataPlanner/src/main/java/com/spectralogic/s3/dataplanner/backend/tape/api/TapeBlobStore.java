/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.api;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.ImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.dataplanner.backend.api.LocalBlobStore;


public interface TapeBlobStore extends LocalBlobStore, TapeEjector
{
    /**
     * Formats the tape specified.  <br><br>
     *
     * <b><font color = red>
     * Warning: This will permanently delete all data on the tape in the tape drive
     * </font></b>
     *
     * @param force        - If true, will format the tape even if it has data on it that we shouldn't delete
     * @param characterize
     * @throws RuntimeException if the specified tape could not be formatted
     */
    void formatTape(final BlobStoreTaskPriority priority, final UUID tapeId, final boolean force, boolean characterize);
    
    
    /**
     * Cancels a pending format tape request that has not yet begun.
     * 
     * @throws RuntimeException if the formatting of the specified tape could not be canceled
     */
    void cancelFormatTape( final UUID tapeId );
    
    
    /**
     * Cancels a pending eject tape request that has not yet begun.
     * 
     * @throws RuntimeException if the ejection of the specified tape could not be canceled
     */
    void cancelEjectTape( final UUID tapeId );
    
    
    /**
     * Brings the tape specified online, moving it from an EE slot to a storage slot or tape drive for
     * inspection.
     * 
     * @throws RuntimeException if the specified tape could not be onlined
     */
    void onlineTape( final UUID tapeId );
    

    /**
     * Cancels a pending online tape request that has not yet begun.
     * 
     * @throws RuntimeException if the onlining of the specified tape could not be canceled
     */
    void cancelOnlineTape( final UUID tapeId );
    
    
    /**
     * Imports the tape specified.  <br><br>
     * 
     * Note: Only tapes with data on them formatted in the way this application expects can be imported.  For
     * example, this method will fail when attempting to import a tape that is LTFS-formatted with a bunch
     * of objects written to it via another application.
     * 
     * @throws RuntimeException if the specified tape could not be imported
     */
    void importTape(
            final BlobStoreTaskPriority priority,
            final ImportTapeDirective directive );
    
    
    /**
     * Imports the tape specified.  <br><br>
     * 
     * @throws RuntimeException if the specified tape could not be imported
     */
    void importTape(
            final BlobStoreTaskPriority priority,
            final RawImportTapeDirective directive );
    

    /**
     * Cancels a pending import tape request that has not yet begun.
     * 
     * @throws RuntimeException if the importing of the specified tape could not be canceled
     */
    void cancelImportTape( final UUID tapeId );
    
    
    /**
     * Inspects the tape specified.
     * 
     * @throws RuntimeException if the specified tape could not be inspected
     */
    void inspectTape( final BlobStoreTaskPriority priority, final UUID tapeId );
    
    
    /**
     * Cleans the drive specified.
     * 
     * @throws RuntimeException if the specified tape could not be cleaned
     */
    void cleanDrive( final UUID driveId );


    /**
     * Tests the drive specified.
     *
     * @throws RuntimeException if the specified tape could not be tested
     */
    void testDrive(final UUID driveId, final UUID tapeId, boolean cleanFirst);
    
    
    /**
     * Cancels a pending verify tape request that has not yet begun.
     * 
     * @throws RuntimeException if the verification of the specified tape could not be canceled
     */
    void cancelVerifyTape( final UUID tapeId );


    /**
     * Cancels a pending test drive request that has not yet begun.
     *
     * @throws RuntimeException if the test of the specified drive could not be canceled
     */
    void cancelTestDrive( final UUID driveId );


    /**
     * Delete an offline tape partition from the database.
     */
    void deleteOfflineTapePartition( final UUID partitionId );


    /**
     * Delete an offline tape drive from the database.
     */
    void deleteOfflineTapeDrive( final UUID tapeDriveId );


    /**
     * Delete a tape from the database (must be in ejected or lost state).
     */
    void deletePermanentlyLostTape( final UUID tapeId );


    /**
     * Flags the tape environment to be refreshed next time its generation is checked. Differs from the
     * "forceTapeEnvironmentRefresh()" call in that it is purely a flag and makes no attempt to synchronously
     * update the environment.
     */
    void flagEnvironmentForRefresh();

    /**
     * Initiates a drive dump
     */
    void driveDump(final UUID driveId);


    void taskSchedulingRequired();
}
