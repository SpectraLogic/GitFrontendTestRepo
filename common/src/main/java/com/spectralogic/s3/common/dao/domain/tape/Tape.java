/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortByIndexNotNeeded;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ Tape.BAR_CODE, Tape.PARTITION_ID }),
    @Unique( SerialNumberObservable.SERIAL_NUMBER )
})
@Indexes( { @Index( Tape.STATE ), @Index( Tape.TYPE ) } )
@SortByIndexNotNeeded
public interface Tape
    extends DatabasePersistable, SerialNumberObservable< Tape >, PersistenceTarget< Tape >
{
    /**
     * The tape serial number is the only immutable, reliable identifier for a tape.  It is optional because
     * we will now know it until we load the tape into a tape drive.
     */
    @Optional
    String getSerialNumber();
    
    
    String BAR_CODE = "barCode";
    
    /**
     * Bar codes are NOT absolutely guaranteed to be unique either globally or even within the context of a 
     * single partition; however, we do enforce uniqueness in the database (if there is a conflict, only a 
     * single tape record will be created for the given bar code and no tape with conflicting bar codes can be
     * used until the conflict is fixed).
     */
    @SortBy( 3 )
    String getBarCode();
    
    Tape setBarCode( final String value );
    
    
    String PARTITION_ID = "partitionId";

    /**
     * Note: This should only be null when the tape has been ejected
     */
    @Optional
    @References( TapePartition.class )
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    UUID getPartitionId();
    
    Tape setPartitionId( final UUID value );
    
    
    String TYPE = "type";

    @SortBy( 2 )
    TapeType getType();
    
    Tape setType( final TapeType value );
    
    
    String LAST_CHECKPOINT = "lastCheckpoint";
    
    /**
     * If one or more {@link Blob}s have been written to the tape, then this value is non-null and is
     * equal to the checkpoint that represents the last quiesce that has all of the written {@link Blob}s.
     * <br><br>
     * 
     * If no {@link Blob}s have been written to the tape, then this value is null.
     */
    @Optional
    String getLastCheckpoint();
    
    Tape setLastCheckpoint( final String value );
    
    
    String TAKE_OWNERSHIP_PENDING = "takeOwnershipPending";
    
    /**
     * @return TRUE if the tape was {@link TapeState#FOREIGN} and was imported when {@link #WRITE_PROTECTED}
     * was true, preventing the tape from being owned.  While in this state, we must check to ensure that the
     * {@link #LAST_CHECKPOINT} of the tape matches what we expect it to be before doing anything.  If there
     * is a mismatch, the tape's state must be changed back to {@link TapeState#FOREIGN} so that it can be
     * re-imported with the appropriate content.
     */
    @DefaultBooleanValue( false )
    boolean isTakeOwnershipPending();
    
    Tape setTakeOwnershipPending( final boolean value );
    
    
    String TOTAL_RAW_CAPACITY = "totalRawCapacity";
    
    @Optional
    Long getTotalRawCapacity();
    
    Tape setTotalRawCapacity( final Long value );
    
    
    String AVAILABLE_RAW_CAPACITY = "availableRawCapacity";
    
    @Optional
    Long getAvailableRawCapacity();
    
    Tape setAvailableRawCapacity( final Long value );
    
    
    String FULL_OF_DATA = "fullOfData";
    
    /**
     * A tape is to be marked as being full of data once either (i) we can no longer write to it, or (ii) the
     * remaining raw capacity is so small that it's not worth writing to it anymore.
     */
    @DefaultBooleanValue( false )
    boolean isFullOfData();
    
    Tape setFullOfData( final boolean value );
    
    
    String WRITE_PROTECTED = "writeProtected";
    
    /**
     * If a tape is physically write-protected, it cannot be written to.  <br><br>
     * 
     * If we do not know if a tape is write-protected or not, it should have false as the value.  If we go to 
     * write to the tape and discover that it is write-protected, we shall update value to true and move onto
     * another tape.
     */
    @DefaultBooleanValue( false )
    boolean isWriteProtected();
    
    Tape setWriteProtected( final boolean value );
    
    
    String PREVIOUS_STATE = "previousState";
    
    /**
     * Some states can be "cancelled" back to the previous state.  For such states, we need the previous state
     * to revert back to.
     */
    @Optional
    TapeState getPreviousState();
    
    Tape setPreviousState( final TapeState state );
    
    
    String STATE = "state";
    
    @SortBy( 1 )
    @DefaultEnumValue( "PENDING_INSPECTION" )
    TapeState getState();
    
    Tape setState( final TapeState state );


    String ROLE = "role";

    /**
     * @return a tape's role, which may specify it is a normal tape or to be used only for test <br><br>
     */
    @DefaultEnumValue( "NORMAL" )
    TapeRole getRole();

    Tape setRole( final TapeRole role );


    String VERIFY_PENDING = "verifyPending";
    
    /**
     * @return the priority for the verify requested, or null if a verify has not been requested <br><br>
     * 
     * Note: If a verify <b>and</b> eject is pending, the eject cannot be processed prior to the verify being
     * processed
     */
    @Optional
    BlobStoreTaskPriority getVerifyPending();
    
    Tape setVerifyPending( final BlobStoreTaskPriority value );
    
    
    String EJECT_PENDING = "ejectPending";
    
    /**
     * @return the date eject was requested, or null if an eject has (i) not been requested, or (ii) an eject
     * has been requested and the eject process has begun and is no longer cancellable
     */
    @Optional
    Date getEjectPending();
    
    Tape setEjectPending( final Date value );
    
    
    String EJECT_DATE = "ejectDate";
    
    @Optional
    Date getEjectDate();
    
    Tape setEjectDate( final Date value );
    
    
    String EJECT_LABEL = "ejectLabel";

    @Optional
    String getEjectLabel();
    
    Tape setEjectLabel( final String value );
    
    
    String EJECT_LOCATION = "ejectLocation";

    @Optional
    String getEjectLocation();
    
    Tape setEjectLocation( final String value );
    
    
    String DESCRIPTION_FOR_IDENTIFICATION = "descriptionForIdentification";
    
    /**
     * When the tape is unknown to us, it will have this property for the user to identify the tape.
     */
    @Optional
    String getDescriptionForIdentification();
    
    Tape setDescriptionForIdentification( final String value );
    
    
    String PARTIALLY_VERIFIED_END_OF_TAPE = "partiallyVerifiedEndOfTape";
    
    /**
     * @return the date the end of the tape was verified, where only a part of the tape is verified (which is
     * user specifiable) at the very end of the tape <br><br>
     * 
     * Note: This date does <b>not</b> get reset to null even if the tape is modified after the end of the 
     * tape is verified <br><br>
     * 
     * Note: There is no guarantee as to how much of the end of the tape was verified on the date it was
     * verified if it was verified, since the percent of the tape to verify at the end is user-specifiable
     * and mutable and the value used at the time the partial verify completed is not recorded on the
     * {@link Tape}
     */
    @Optional
    Date getPartiallyVerifiedEndOfTape();
    
    Tape setPartiallyVerifiedEndOfTape( final Date value );


    String ALLOW_ROLLBACK = "allowRollback";

    /**
     * @return TRUE if the tape is allowed to rollback on its next verifyQuiescedToCheckpoint call, FALSE otherwise <br><br>
     *
     * Note: This value is set to TRUE when the tape is being quiesced and is set to FALSE when the tape is
     * done being quiesced. It is also set to false after a successful verifyQuiescedToCheckpoint.
     */
    @DefaultBooleanValue( false )
    boolean isAllowRollback();

    Tape setAllowRollback(final boolean value );


    String CHARACTERIZATION_VER = "characterizationVer";

    @Optional
    String getCharacterizationVer();

    Tape setCharacterizationVer(final String value );
}
