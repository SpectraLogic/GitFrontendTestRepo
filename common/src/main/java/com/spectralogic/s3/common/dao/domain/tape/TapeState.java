/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.util.lang.Validations;

public enum TapeState
{
    /**
     * The only state that permits the tape to have data read off of it, be used to write data, or be assigned
     * to a bucket.  Tapes in a normal state may or may not already be assigned to a bucket.
     */
    NORMAL( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.YES, 
            Cancellable.NO ),
    
    
    /**
     * Tape was detected in an EE slot and requires customer affirmation to bring it online in the tape 
     * partition.  OFFLINE tapes cannot be operated on in any way.  An OFFLINE tape must be brought online
     * via a user action, at which point the tape's state becomes ONLINE_PENDING.
     */
    OFFLINE(
            PhysicallyPresent.NO,
            LoadIntoDriveAllowed.NO,
            TapeCommandsAllowed.NO, 
            Cancellable.NO,
            PreviousStateTracking.FORCED ),
    
    
    /**
     * Tape was OFFLINE and received user confirmation to bring it online, but this action has not yet begun.
     */
    ONLINE_PENDING( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.NO,
            Cancellable.NO,
            PreviousStateTracking.FORCED ),
    
    
    /**
     * Tape is in the process of being moved away from its EE slot, its state will change to 
     * PENDING_INSPECTION.
     */
    ONLINE_IN_PROGRESS(
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.NO, 
            Cancellable.NO, 
            PreviousStateTracking.FORCED ),
    
    
    /**
     * Tape has not yet been inspected.
     */
    PENDING_INSPECTION( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.YES, 
            Cancellable.NO, 
            PreviousStateTracking.FORCED, 
            CanBePreviousState.YES ),
    
    
    /**
     * Tape has data on it, or is otherwise unavailable to the system.
     */
    UNKNOWN( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.YES, 
            Cancellable.NO ),
    
    
    /**
     * Tape was supposed to have data on it that this application recognizes, but there was a failure
     * attempting to verify that we were at the checkpoint that contains said data, or a failure rolling back
     * to said checkpoint, while the tape was not in a read-only state.
     */
    DATA_CHECKPOINT_FAILURE(
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.FOR_INSPECT_ONLY,
            TapeCommandsAllowed.YES,
            Cancellable.NO ),
            
            
    /**
     * Tape was supposed to have data on it that this application recognizes, but there was a failure
     * attempting to verify that we were at the checkpoint that contains said data, or a failure rolling back
     * to said checkpoint, while the tape was in a read-only state (meaning that corrective action didn't
     * have any chance to succeed).
     */
    DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY(
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.FOR_INSPECT_ONLY,
            TapeCommandsAllowed.YES,
            Cancellable.NO ),
    
    
    /**
     * Tape was supposed to have data on it that this application recognizes, but the checkpoint that contains
     * said data could not be found or rolled back to on the tape.
     */
    DATA_CHECKPOINT_MISSING(
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.FOR_INSPECT_ONLY,
            TapeCommandsAllowed.YES, 
            Cancellable.NO ),
    
    
    /**
     * An LTFS-formatted tape not from another Bluestorm system with LTFS data on it.  This state differs from
     * the {@link #FOREIGN} state in that the data on the LTFS tape follows the LTFS standard and is not
     * formatted or laid out on the tape in the way Bluestorm data would be.  This data must be copied into a
     * bucket (which will result in new tapes being assigned and written to) before being accessible thru 
     * Bluestorm.
     */
    LTFS_WITH_FOREIGN_DATA( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES,
            TapeCommandsAllowed.YES, 
            Cancellable.NO ),
            
            
    /**
     * For raw imports only.  An import is said to be raw if the tape to import is not formatted and written
     * to in a Bluestorm structure.  The "raw" refers to the fact that we must look at raw, non-Bluestorm-
     * structured data on the tape and attempt to import it.  <br><br>
     * 
     * When an import is initiated, the tape will transist to this state until the import begins.  An import
     * that transists the tape into this state can only be initiated on a 
     * {@link TapeState#LTFS_WITH_FOREIGN_DATA} tape.
     */
    RAW_IMPORT_PENDING( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES,
            TapeCommandsAllowed.NO, 
            Cancellable.YES ),
            
            
    /**
     * For raw imports only.  An import is said to be raw if the tape to import is not formatted and written
     * to in a Bluestorm structure.  The "raw" refers to the fact that we must look at raw, non-Bluestorm-
     * structured data on the tape and attempt to import it.  <br><br>
     * 
     * Upon successful completion, the tape state will transist to <code>NORMAL</code>.  Upon failure, the
     * import will be retried a number of times, and eventually, the tape state will transist back to 
     * {@link TapeState#LTFS_WITH_FOREIGN_DATA};
     */
    RAW_IMPORT_IN_PROGRESS( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES,
            TapeCommandsAllowed.NO, 
            Cancellable.NO ),
    
    
    /**
     * A tape from another Bluestorm system, unrecognized by this Bluestorm system.  This is the only state
     * from which an import may be initiated, which includes the tape into a bucket without having to copy all
     * the data on the tape off onto other tapes, as is required when ingressing data from a tape in state
     * {@link #LTFS_WITH_FOREIGN_DATA}.
     */
    FOREIGN( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.YES,
            Cancellable.NO ),
            
            
    /**
     * When an import is initiated, the tape will transist to this state until the import begins.  An import
     * that transists the tape into this state can only be initiated on a {@link TapeState#FOREIGN} tape.
     */
    IMPORT_PENDING( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES,
            TapeCommandsAllowed.NO, 
            Cancellable.YES ),
    
    
    /**
     * Upon successful completion, the tape state will transist to <code>NORMAL</code>.  Upon failure, the
     * import will be retried a number of times, and eventually, the tape state will transist back to 
     * {@link TapeState#FOREIGN}.
     */
    IMPORT_IN_PROGRESS( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES,
            TapeCommandsAllowed.NO, 
            Cancellable.NO ),
            
            
    /**
     * The tape is not compatible with the tape partition it resides in and must be ejected.
     */
    INCOMPATIBLE( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.NO,
            TapeCommandsAllowed.NO, 
            Cancellable.NO ),
    
    
    /**
     * Tape that has been destroyed, stolen, misplaced...etc.  The system needs to know about these tapes as 
     * they may have to be rebuilt from a redundant media.  The system also needs to know as it can respond 
     * with the correct error message.
     */
    LOST( 
            PhysicallyPresent.NO,
            LoadIntoDriveAllowed.NO, 
            TapeCommandsAllowed.NO,
            Cancellable.NO,
            PreviousStateTracking.FORCED ),
    
    
    /**
     * Tape that has been identified as bad (e.g. due to too many write cycles or I/O errors).  Tape should
     * not be allocated or used for anything and should be ejected and replaced.
     */
    BAD( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.NO,
            TapeCommandsAllowed.NO,
            Cancellable.NO ),
            
            
    /**
     * Tape needs to be formatted, but can't be since it's write protected.
     */
    CANNOT_FORMAT_DUE_TO_WRITE_PROTECTION( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.NO,
            TapeCommandsAllowed.NO,
            Cancellable.NO ),
    
    
    /**
     * Tape that has a serial number that does not match what it should be.  This usually means that the bar
     * code of the tape got changed by mistake.  It could also mean that there was a tape library bug where
     * the library moved the wrong tape into the drive and then we read the serial number and found the 
     * mismatch.
     */
    SERIAL_NUMBER_MISMATCH( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.NO, 
            TapeCommandsAllowed.NO, 
            Cancellable.YES ),
    
    
    /**
     * The bar code for the tape is unknown or missing.  If the tape is in a drive, it should be moved to
     * storage.  It is possible we'll determine the bar code at that time.  If the tape is in storage, it
     * cannot be used in any way whatsoever.
     */
    BAR_CODE_MISSING( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.NO, 
            TapeCommandsAllowed.NO,
            Cancellable.NO ),
    
    
    /**
     * This tape is sparsely populated and its data is being read and re-written to other tapes 
     */
    AUTO_COMPACTION_IN_PROGRESS( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.YES, 
            Cancellable.NO ),
    
    
    /**
     * A format has been requested and is pending for this tape.  The format has not yet begun.
     */
    FORMAT_PENDING( 
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.NO, 
            Cancellable.YES ),

        
    /**
     * A format is in progress.  Once the format is completed, the tape's state should transist to
     * <code>NORMAL</code>.
     */
    FORMAT_IN_PROGRESS(
            PhysicallyPresent.YES,
            LoadIntoDriveAllowed.YES, 
            TapeCommandsAllowed.NO,
            Cancellable.NO,
            PreviousStateTracking.FORCED ),
    
    
    /**
     * We are in the process of moving the tape to an EE slot so that it can be ejected.  Once this move
     * completes, the tape's state will transist to <code>EJECT_FROM_EE_PENDING</code>.
     */
    EJECT_TO_EE_IN_PROGRESS(
            PhysicallyPresent.NO,
            LoadIntoDriveAllowed.NO, 
            TapeCommandsAllowed.NO, 
            Cancellable.NO,
            PreviousStateTracking.FORCED ),
    
    
    /**
     * The tape is in an EE slot waiting to be physically ejected.  Once physically ejected, the tape's state
     * will transist to <code>EJECTED</code>.
     */
    EJECT_FROM_EE_PENDING(
            PhysicallyPresent.NO,
            LoadIntoDriveAllowed.NO, 
            TapeCommandsAllowed.NO,
            Cancellable.NO, 
            PreviousStateTracking.FORCED ),
    
    
    /**
     * Ejected from the system / not physically present.
     */
    EJECTED( 
            PhysicallyPresent.NO,
            LoadIntoDriveAllowed.NO, 
            TapeCommandsAllowed.NO,
            Cancellable.NO,
            PreviousStateTracking.FORCED ),
    ;
    
    
    private TapeState( 
            final PhysicallyPresent physicallyPresent,
            final LoadIntoDriveAllowed usable,
            final TapeCommandsAllowed canPerformTapeCommands, 
            final Cancellable cancellable )
    {
        this( physicallyPresent, usable, canPerformTapeCommands, cancellable, PreviousStateTracking.DEFAULT );
    }
    
    
    private TapeState( 
            final PhysicallyPresent physicallyPresent,
            final LoadIntoDriveAllowed usable,
            final TapeCommandsAllowed canPerformTapeCommands, 
            final Cancellable cancellable,
            final PreviousStateTracking previousStateTracking )
    {
        this( physicallyPresent, usable, canPerformTapeCommands, cancellable, previousStateTracking, null );
    }
    
    
    private TapeState( 
            final PhysicallyPresent physicallyPresent,
            final LoadIntoDriveAllowed usable,
            final TapeCommandsAllowed canPerformTapeCommands, 
            final Cancellable cancellable,
            final PreviousStateTracking previousStateTracking,
            final CanBePreviousState canBePreviousState )
    {
        Validations.verifyNotNull( "Physically present", physicallyPresent );
        Validations.verifyNotNull( "Usable", usable );
        Validations.verifyNotNull( "Can perform tape commands", canPerformTapeCommands );
        Validations.verifyNotNull( "Cancellable", cancellable );
        Validations.verifyNotNull( "Previous state tracking", previousStateTracking );
        m_physicallyPresent = ( PhysicallyPresent.YES == physicallyPresent );
        m_allowedToBeLoadedIntoDrive = ( LoadIntoDriveAllowed.YES == usable );
        m_allowedToBeInspected = ( LoadIntoDriveAllowed.NO != usable );
        m_cancellable = ( Cancellable.YES == cancellable );
        m_canPerformTapeCommands = ( TapeCommandsAllowed.YES == canPerformTapeCommands );
        m_previousStateTracked = ( PreviousStateTracking.FORCED == previousStateTracking || m_cancellable );
        m_canBePreviousState = ( null == canBePreviousState ) ? 
                !m_previousStateTracked
                : ( CanBePreviousState.YES == canBePreviousState );
    }
    
    
    private enum PhysicallyPresent
    {
        YES,
        NO
    }
    
    
    private enum LoadIntoDriveAllowed
    {
        YES,
        FOR_INSPECT_ONLY,
        NO
    }
    
    
    private enum TapeCommandsAllowed
    {
        YES,
        NO
    }
    
    
    private enum Cancellable
    {
        YES,
        NO
    }
    
    
    /**
     * Specifies the policy for whether or not the previous state should be tracked.
     */
    public enum PreviousStateTracking
    {
        /**
         * The previous state will only be tracked if the state is cancellable.
         */
        DEFAULT,
        
        /**
         * The previous state will be tracked, regardless as to whether the new state is cancellable.
         */
        FORCED,
    }
    
    
    private enum CanBePreviousState
    {
        YES,
        NO
    }
    
    
    /**
     * Some states indicate that the tape is not physically present (for example, ejected or lost).  Tapes
     * that have a state that denotes that it is not physically present cannot have any tape operations
     * performed on them, nor can they be loaded into a tape drive for any reason.
     */
    public boolean isPhysicallyPresent()
    {
        return m_physicallyPresent;
    }
    
    
    /**
     * Some states prohibit the system from doing anything with the tape - other than move it (for example, to
     * eject it).  For example, if a tape is in a bad or otherwise corrupt state, the tape is said to be 
     * unusable in the sense that it cannot be loaded up in a tape drive to perform any task against it.
     * <br><br>
     * 
     * If a tape is allowed to be loaded into a drive for a given state, then that state is inherently
     * physically present.
     */
    public boolean isLoadIntoDriveAllowed()
    {
        return m_allowedToBeLoadedIntoDrive;
    }
    
    
    /**
     * Some states prohibit the system from inspecting the tape.  <br><br>
     * 
     * A tape that cannot be inspected cannot be loaded into a drive; however, not all tapes that can't be
     * loaded into a drive can't be inspected.  Or, in other words, sometimes a tape that can't be loaded into
     * a drive can be inspected, and if inspected, its state shall be changed to PENDING_INSPECTION.
     */
    public boolean isInspectionAllowed()
    {
        return m_allowedToBeInspected;
    }
    
    
    /**
     * Some states will not permit tape commands (eject, format, import, etc.) while that tape holds that 
     * state.  <br><br>
     * 
     * If tape commands can be performed for a given state, then that state is inherently physically present.
     * The state is not however inherently allowed to be loaded into a drive.  This is because performing a
     * tape command can change a tape's state such that the new state is allowed to be loaded into a drive.
     */
    public boolean isTapeCommandAllowed()
    {
        return m_canPerformTapeCommands;
    }
    
    
    /**
     * A state is cancellable if state can be cancelled and reverted back to its previous state via user 
     * action.  Note that just because a state is cancellable does not mean that by the time you request the 
     * cancel that it is still cancellable.
     */
    public boolean isCancellableToPreviousState()
    {
        return m_cancellable;
    }
    
    
    /**
     * If this state should have its previous state tracked, returns true.  <br><br>
     */
    public boolean isPreviousStateTracked()
    {
        return m_previousStateTracked;
    }
    
    
    /**
     * Only states that report that they can be a previous state can be used as the previous state for a tape.
     */
    public boolean canBePreviousState()
    {
        return m_canBePreviousState;
    }
    
    
    /**
     * @return all states that denote lack of physical presence
     */
    public static Set< TapeState > getStatesThatAreNotPhysicallyPresent()
    {
        final Set< TapeState > retval = new HashSet<>();
        for ( final TapeState state : values() )
        {
            if ( state.isPhysicallyPresent() )
            {
                continue;
            }
            retval.add( state );
        }
        return retval;
    }
    
    
    /**
     * @return all states that disallow loading a tape into a drive
     */
    public static Set< TapeState > getStatesThatDisallowTapeLoadIntoDrive()
    {
        final Set< TapeState > retval = new HashSet<>();
        for ( final TapeState state : values() )
        {
            if ( state.isLoadIntoDriveAllowed() )
            {
                continue;
            }
            retval.add( state );
        }
        return retval;
    }
    
    
    private final boolean m_physicallyPresent;
    private final boolean m_allowedToBeLoadedIntoDrive;
    private final boolean m_allowedToBeInspected;
    private final boolean m_cancellable;
    private final boolean m_canPerformTapeCommands;
    private final boolean m_previousStateTracked;
    private final boolean m_canBePreviousState;
}
