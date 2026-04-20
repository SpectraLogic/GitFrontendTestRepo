/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.util.lang.Validations;
import org.apache.log4j.Logger;

public final class TapeAvailabilityImpl implements TapeAvailability
{
    public TapeAvailabilityImpl(
            final TapeDrive drive,
            final Set<UUID> tapesInAvailablePartition,
            final TapeLockSupport<?> tapeLockSupport,
            final Set<UUID> tapesNotInPartitionDriveAvailabilityIsIn,
            final Set<UUID> tapesInUnavailableState,
            final Set<UUID> inaccessibleTapes,
            final Set<UUID> tapesAttemptedTooManyTimesOnDrive,
            final boolean allowRecentlyUnlockedTapes )
    {
        m_tapePartitionId = drive.getPartitionId();
        m_drive = drive.getId();
        m_permanentlyUnavailableTapes.addAll( tapesInUnavailableState );

        //Assure we have no nulls, as these cause problems for "ANY" queries
        tapesInAvailablePartition.remove(null);
        tapesNotInPartitionDriveAvailabilityIsIn.remove(null);
        tapesInUnavailableState.remove(null);
        inaccessibleTapes.remove(null);
        tapesAttemptedTooManyTimesOnDrive.remove(null);

        final StringBuilder sb = new StringBuilder();
        final Set<UUID> lockedTapes = tapeLockSupport.getLockedTapes( null );
        final Set<UUID> recentlyUnlockedTapes = tapeLockSupport.getRecentlyUnlocked();
        if ( !lockedTapes.isEmpty() ) {
            sb.append( lockedTapes.size() );
            sb.append( " locked " );
            m_temporarilyUnavailableTapes.addAll( lockedTapes );
        }
        //NOTE: Since we do a "deploy tasks without moving tapes" pass followed by a "deploy even if it requires moving"
        //pass in TapeBlobStoreProcessorImpl, we employ this check to assure that we have at least considered this tape
        // for a "non moving" pass before trying it in a "moving" pass. Otherwise we might be considering a tape that
        // just this moment became available in a different drive from this one during our "moving" pass. We exclude
        // whatever tape is in the current drive from this check since it would not need to be moved.
        recentlyUnlockedTapes.remove(drive.getTapeId());
        if ( !allowRecentlyUnlockedTapes && !recentlyUnlockedTapes.isEmpty() ) {
            sb.append( recentlyUnlockedTapes.size() );
            sb.append( " will be checked next iteration " );
            m_temporarilyUnavailableTapes.addAll( recentlyUnlockedTapes );
        }
        if (!tapesNotInPartitionDriveAvailabilityIsIn.isEmpty()) {
            sb.append(tapesNotInPartitionDriveAvailabilityIsIn.size());
            sb.append(" not in this partition ");
            m_temporarilyUnavailableTapes.addAll( tapesNotInPartitionDriveAvailabilityIsIn );
        }
        if (!inaccessibleTapes.isEmpty()) {
            sb.append(inaccessibleTapes.size());
            sb.append(" inaccessible ");
            m_temporarilyUnavailableTapes.addAll( inaccessibleTapes );
        }
        if (!tapesAttemptedTooManyTimesOnDrive.isEmpty()) {
            sb.append(tapesAttemptedTooManyTimesOnDrive.size());
            sb.append(" had too many failures on this drive ");
            m_temporarilyUnavailableTapes.addAll( tapesAttemptedTooManyTimesOnDrive );
        }
        m_temporarilyUnavailableTapes.removeAll( m_permanentlyUnavailableTapes );
        m_tempUnavailabilityReasons = sb.toString();

        m_availableTapes.addAll( tapesInAvailablePartition );
        m_availableTapes.removeAll( m_permanentlyUnavailableTapes );
        m_availableTapes.removeAll( m_temporarilyUnavailableTapes );


        m_tapeLockSupport = tapeLockSupport;
        
        UUID preferredTape = drive.getTapeId();
        if ( null != preferredTape && !m_availableTapes.contains( preferredTape ) )
        {
            preferredTape = null;
        }
        m_tapeInDrive = preferredTape;
    }
    
    
    public TapeAvailabilityImpl( final UUID tapePartitionId, final UUID driveId, final UUID tapeInDrive )
    {
        Validations.verifyNotNull( "Drive", driveId );
        Validations.verifyNotNull( "Tape", tapeInDrive );
        Validations.verifyNotNull( "Partition", tapePartitionId );
        m_tapeLockSupport = null;
        m_tapePartitionId = tapePartitionId;
        m_drive = driveId;
        m_tapeInDrive = tapeInDrive;
        m_availableTapes.add(m_tapeInDrive);
        m_tempUnavailabilityReasons = "";
    }
    
    
    public UUID getTapePartitionId()
    {
        return m_tapePartitionId;
    }
    
    
    public UUID getDriveId()
    {
        return m_drive;
    }
    
    
    public UUID getTapeInDrive()
    {
        return m_tapeInDrive;
    }

    
    public Set< UUID > getAvailableTapes()
    {
        return new HashSet<>( m_availableTapes );
    }
    
    
    public Set< UUID > getTemporarilyUnavailableTapes()
    {
        return new HashSet<>( m_temporarilyUnavailableTapes );
    }
    
    
    public Set< UUID > getPermanentlyUnavailableTapes()
    {
        return new HashSet<>( m_permanentlyUnavailableTapes );
    }
    
    
    public Set< UUID > getAllUnavailableTapes()
    {
        final Set< UUID > retval = new HashSet<>();
        retval.addAll( m_temporarilyUnavailableTapes );
        retval.addAll( m_permanentlyUnavailableTapes );
        return retval;
    }
    
    
    public String verifyAvailable( final UUID tapeId )
    {
        try {
            if (null == tapeId) {
                return null;
            }
            if (getAvailableTapes().contains(tapeId)) {
                return null;
            }
            if (getPermanentlyUnavailableTapes().contains(tapeId)) {
                throw new IllegalStateException("Tape " + tapeId + " is permanently unavailable.");
            }
            if (getTemporarilyUnavailableTapes().contains(tapeId)) {
                final Object lockHolder = m_tapeLockSupport.getTapeLockHolder(tapeId);
                if (null != lockHolder) {
                    return "locked by " + lockHolder;
                }
                //TODO: we should categorize with more granularity than this so we can report reason unambiguously.
                return "not in partition, or temporarily unavailable";
            }
            throw new IllegalStateException("Tape " + tapeId + " is unknown.");
        } catch (final Exception e) {
            LOG.warn("Tape availability summary: " + getSummary());
            throw e;
        }
    }


    public String getSummary() {
        final StringBuilder sb = new StringBuilder();
        sb.append(m_availableTapes.size());
        sb.append(" tapes available. ");
        if (!m_permanentlyUnavailableTapes.isEmpty()) {
            sb.append(m_permanentlyUnavailableTapes.size());
            sb.append(" tapes in unavailable state. ");
        }
        //TODO: currently we print summaries when availability is a problem, but it would be better ot get
        //the specific reason a given tape was unavailable and report it in the error*/
        if (!m_temporarilyUnavailableTapes.isEmpty()) {
            sb.append(m_temporarilyUnavailableTapes.size());
            sb.append(" tapes temporarily unavailable [");
            sb.append(m_tempUnavailabilityReasons);
            sb.append("]");
        }
        return sb.toString();
    }
    
    
    private final UUID m_tapeInDrive;
    private final UUID m_drive;
    private final UUID m_tapePartitionId;
    private final TapeLockSupport< ? > m_tapeLockSupport;
    private final Set< UUID > m_availableTapes = new HashSet<>();
    private final Set< UUID > m_temporarilyUnavailableTapes = new HashSet<>();
    private final Set< UUID > m_permanentlyUnavailableTapes = new HashSet<>();
    private final String m_tempUnavailabilityReasons;
    private final static Logger LOG = Logger.getLogger( TapeAvailabilityImpl.class );
}
