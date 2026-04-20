/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class InspectTapeTask extends BaseTapeTask implements StaticTapeTask
{
    public InspectTapeTask(final BlobStoreTaskPriority priority, final UUID tapeId, TapeFailureManagement tapeFailureManagement, final BeansServiceManager serviceManager)
    {
        super( priority, tapeId, tapeFailureManagement, serviceManager );
    }
    
    
    @Override
    protected void performPreRunValidations()
    {
        verifyTapeInDrive( new DefaultTapeInDriveVerifier(this, true ) );
    }

    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        final TapeDrive tapeDrive = getServiceManager().getService( TapeDriveService.class ).attain( getDriveId() );
        Tape tape = getTape();

        if ( !tapeDrive.getType().getSupportedTapeTypes().contains( tape.getType() ) )
        {
            LOG.info("Tape " + tape.getId() + " has incompatible type " + tape.getType() + " with drive " +
                    getDriveId() + " of type " + tapeDrive.getType() );
            getTapeService().transistState( getTape(), TapeState.INCOMPATIBLE );
            return BlobStoreTaskState.COMPLETED;
        }

        boolean success = false;
        String tapeDescriptionForIdentification;
        try
        {
            tapeDescriptionForIdentification = TapeTaskUtils.inspect(
                    getTape(),
                    getDriveId(),
                    getDriveResource(),
                    m_tapeFailureManagement );
            success = true;
        }
        catch ( final RpcException ex )
        {
            tapeDescriptionForIdentification = "Failed to inspect tape";
        }
        
        tape = getTape();
        if ( TapeState.PENDING_INSPECTION == tape.getState() && null != tape.getPreviousState() )
        {
            LOG.info( "This tape has a previous state of " + tape.getPreviousState() 
                      + ", indicating that the tape was lost or ejected and is now being reconciled." );
            getTapeService().rollbackLastStateTransition( tape );
        }
        
        final LoadedTapeInformation tapeInformation =
                getDriveResource().getLoadedTapeInformation().get( Timeout.LONG );
        final boolean foreignAndInconsistent = ( !success 
                        && null != tapeInformation.getTapeId()
                        && !tapeInformation.getTapeId().equals( tape.getId() ) );
        if ( foreignAndInconsistent )
        {
            LOG.warn( "Tape is foreign and failed inspection (it is likely inconsistent)." );
            handleOwnedTapeWithData( tape, tapeDescriptionForIdentification, tapeInformation.getTapeId() );
        }
        else if ( !success )
        {
            handleInspectionFailure( tape );
        }
        else if ( null == tapeDescriptionForIdentification )
        {
            handleBlankTape( tape, tapeInformation );
        }
        else if ( null != tapeInformation.getTapeId() )
        {
            handleOwnedTapeWithData( tape, tapeDescriptionForIdentification, tapeInformation.getTapeId() );
        }
        else if ( tape.isAssignedToStorageDomain() )
        {
            //NOTE: currently, this assumes the tape hasn't been changed by a non-BP application since import
            LOG.warn( "Tape " + tape.getId()  + " is assigned to a storage domain, but doesn't have " +
                    "a tape ID written to it, which indicates that it as an LTFS Foreign tape that" +
                    " we've already imported." );
            handleOwnedTapeWithData( tape, tapeDescriptionForIdentification, tape.getId() );
        }
        else
        {
            handleUnownedTapeWithData( tape, tapeDescriptionForIdentification );
        }
        
        final Tape updatedTape = getTape();
        if ( null == updatedTape.getTotalRawCapacity() )
        {
            if ( TapeState.NORMAL == tape.getState() )
            {
                LOG.warn( "Must re-inspect " + updatedTape
                        + " since tape extended attributes aren't populated." );
                getTapeService().transistState( updatedTape, TapeState.PENDING_INSPECTION );
            }
            else
            {
                LOG.info( "Will not re-inspect tape " + updatedTape.getId() + " (" + updatedTape.getBarCode()
                          + ") even though extended attributes aren't populated since tape is in state " 
                          + updatedTape.getState() + "." );
            }
        }
        
        return BlobStoreTaskState.COMPLETED;
    }
    
    
    private void handleBlankTape( 
            final Tape tape, 
            final LoadedTapeInformation tapeInformation )
    {
        final boolean currentStateCancellable = tape.getState().isCancellableToPreviousState();
        if ( currentStateCancellable )
        {
            if ( TapeState.NORMAL == tape.getPreviousState() )
            {
                if ( null == tapeInformation.getTapeId() 
                        || !tapeInformation.getTapeId().equals( getTapeId() ) )
                {
                    LOG.info( "Deferring inspection since the tape's state is " + tape.getState() + "." );
                    getTapeService().updatePreviousState( tape, TapeState.PENDING_INSPECTION );
                }
                else
                {
                    LOG.info( "Empty tape's previous state is normal, so will leave it alone." );
                }
            }
            else
            {
                LOG.info( "Deferring inspection since the tape's state is " + tape.getState() + "." );
                getTapeService().updatePreviousState( tape, TapeState.PENDING_INSPECTION );
            }
            return;
        }        
        
        final Set<BlobTape> blobTapeRecords = getServiceManager().getRetriever( BlobTape.class )
                .retrieveAll(
                        Require.exists(
                                BlobTape.TAPE_ID,
                                Require.beanPropertyEquals(
                                        SerialNumberObservable.SERIAL_NUMBER,
                                        tapeInformation.getSerialNumber() )
                                    ) ).toSet();
        if ( !blobTapeRecords.isEmpty() )
        {
            LOG.warn( blobTapeRecords.size() + " blobs were expected on tape "
                    + getTape().getSerialNumber() + ", but it appears to be blank. Setting tape state to" +
                    " \"BAD\" in case data recovery is needed." );
            getTapeService().transistState( getTape(), TapeState.BAD );
        }
        else
        {
            final TapeDriveType tapeDriveType = getServiceManager().getRetriever( TapeDrive.class )
                                                                   .attain( getDriveId() )
                                                                   .getType();
    
            try
            {
                verifyTapeIsWritable();
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Cannot format tape since the tape is write-protected.", ex );
                getTapeService().transistState( getTape(), TapeState.CANNOT_FORMAT_DUE_TO_WRITE_PROTECTION );
                return;
            }
    
            if ( tapeDriveType.isWriteSupported( tape.getType() ) )
            {
                if ( null == tapeInformation.getTapeId() )
                {
                    format( tape, "has no ownership and no associated blobs" );
                }
                else if ( !tapeInformation.getTapeId()
                                          .equals( getTapeId() ) )
                {
                    format( tape, "has foreign ownership and no associated blobs" );
                }
                else if ( TapeState.NORMAL == tape.getState() )
                {
                    LOG.info( "Empty tape is in a normal state with no associated blobs, so will leave it alone." );
                }
                else
                {
                    format( tape, "has state " + tape.getState() + " with no associated blobs " );
                }
            }
            else
            {
                m_tapeFailureManagement.registerFailure(
                        getTapeId(),
                        TapeFailureType.INCOMPATIBLE,
                        new RuntimeException(
                                "Unable to format tape " + tape.getBarCode() + " of type " + tape.getType() + " in drive type" +
                                        tapeDriveType + " as it's incompatible." ) );
                getTapeService().transistState( getTape(), TapeState.INCOMPATIBLE );
            }
        }
    }
    
    
    private void handleOwnedTapeWithData( 
            final Tape tape,
            final String tapeDescriptionForIdentification,
            final UUID tapeId )
    {
        final boolean foreign = !tape.getId().equals( tapeId ) && !tape.isTakeOwnershipPending();
        final TapeState state = ( foreign ) ? TapeState.FOREIGN : TapeState.NORMAL;
        final boolean currentStateCancellable = tape.getState().isCancellableToPreviousState();
        if ( state == tape.getState() )
        {
            LOG.info( "Non-empty tape is " + state + ", so will leave it alone." );
        }
        else if ( currentStateCancellable )
        {
            if ( state == tape.getPreviousState() )
            {
                LOG.info( "Non-empty tape's previous state is " + state + ", so will leave it alone." );
            }
            else
            {
                getTapeService().updatePreviousState( tape, state );
            }
        }
        else
        {
            getTapeService().transistState( tape, state );
        }
        updateTapeInformation( tape, tapeDescriptionForIdentification );
    }
    
    
    private void handleUnownedTapeWithData( final Tape tape, final String tapeDescriptionForIdentification )
    {
        final boolean currentStateCancellable = tape.getState().isCancellableToPreviousState();
        if ( currentStateCancellable )
        {
            getTapeService().updatePreviousState( tape, TapeState.LTFS_WITH_FOREIGN_DATA );
        }
        else
        {
            getTapeService().transistState( tape, TapeState.LTFS_WITH_FOREIGN_DATA );
        }
        updateTapeInformation( tape, tapeDescriptionForIdentification );
    }
    
    
    private void handleInspectionFailure( final Tape tape )
    {
        final boolean currentStateCancellable = tape.getState().isCancellableToPreviousState();
        final TapeState newTapeState = ( null == tape.getLastCheckpoint() ) ? 
                TapeState.UNKNOWN
                : ( tape.isWriteProtected() ) ? 
                        TapeState.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY
                        : TapeState.DATA_CHECKPOINT_FAILURE;
        if ( currentStateCancellable )
        {
            getTapeService().updatePreviousState( tape, newTapeState );
        }
        else
        {
            getTapeService().transistState( tape, newTapeState );
        }
    }
    
    
    private void format( final Tape tape, final String details )
    {
        final FormatTapeTask formatTask = new FormatTapeTask( getPriority(), getTapeId(), m_tapeFailureManagement, m_serviceManager );
        LOG.info( "The tape is blank and " + details + ".  Will format it using task #" + formatTask.getId() 
                  + "." );
        getTapeService().transistState( tape, TapeState.FORMAT_PENDING );
        formatTask.prepareForExecutionIfPossible( 
                getDriveResource(), 
                new TapeAvailabilityImpl( getTapePartitionId(), getDriveId(), getTapeId() ) );
        
        try
        {
            formatTask.run();
            if ( TapeState.NORMAL != getTape().getState() )
            {
                throw new RuntimeException( "Tape state after a format call wasn't normal." );
            }
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to format tape " + getTapeId() + ".", ex );
            getTapeService().rollbackLastStateTransition( getTape() );
        }
    }
    
    
    private void updateTapeInformation( final Tape tape, final String tapeDescriptionForIdentification )
    {
        tape.setDescriptionForIdentification( tapeDescriptionForIdentification );
        getTapeService().update( tape, Tape.DESCRIPTION_FOR_IDENTIFICATION );
        updateTapeExtendedInformation();
    }
    
    
    private TapeService getTapeService()
    {
        return getServiceManager().getService( TapeService.class );
    }
    
    
    public String getDescription()
    {
        return "Inspect Tape " + m_defaultTapeId;
    }


    @Override
    public boolean allowMultiplePerTape() {
        return false;
    }
}
