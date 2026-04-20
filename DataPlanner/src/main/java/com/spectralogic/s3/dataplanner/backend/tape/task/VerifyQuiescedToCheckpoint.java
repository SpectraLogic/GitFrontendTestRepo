/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeResourceFailureCode;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.FailureHandling;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.RestoreExpected;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.util.UUID;

final class VerifyQuiescedToCheckpoint
{
    VerifyQuiescedToCheckpoint(
            final Tape tape,
            final TapeDriveResource tdResource,
            final BeansServiceManager serviceManager,
            final TapeFailureManagement tapeFailureManagement,
            final RestoreExpected recordTapeFailure,
            final FailureHandling failureHandling)
    {
        m_tape = tape;
        m_tdResource = tdResource;
        m_serviceManager = serviceManager;
        m_recordTapeFailure = recordTapeFailure;
        m_failureHandling = failureHandling;
        
        Validations.verifyNotNull( "Tape", tape );
        Validations.verifyNotNull( "Tape drive resource", tdResource );
        Validations.verifyNotNull( "Service manager", serviceManager );
        Validations.verifyNotNull( "Record tape failure", recordTapeFailure );
        Validations.verifyNotNull( "Failure handling", failureHandling );
        m_tapeDescription = m_tape.getId() + " (" + m_tape.getBarCode() + ")";
        m_tapeFailureManagement = tapeFailureManagement;
        m_driveId = new TapeRM( m_tape, m_serviceManager ).getTapeDrives().getFirst().getId();
        
        // Nothing to do if there is no last checkpoint to verify against
        if ( null == m_tape.getLastCheckpoint() )
        {
            m_lti = null;
            return;
        }
        
        // If we can't get the loaded tape information, we can't proceed
        m_lti = getLoadedTapeInformation();
        if ( null == m_lti )
        {
            return;
        }
        
        // Only verify quiesced to checkpoint, which can modify the tape, if the tape is owned by us
        if ( m_tape.getId().equals( m_lti.getTapeId() ) )
        {
            verifyQuiescedToCheckpoint();
            return;
        }
        
        // If we're dealing with a foreign tape, do not touch it
        if ( !m_tape.isTakeOwnershipPending() )
        {
            TapeTaskUtils.LOG.info(
                    "Will not attempt to verify quiesced to checkpoint since tape is foreign." );
            return;
        }
        
        /*
         * We're dealing with a tape that isn't owned by us, but has been imported and isn't treated as being
         * foreign.  This can only happen if the tape was read-only when it was imported.  Eventually, we
         * will either (i) take ownership of it when it becomes writable, or (ii) change its state back to
         * foreign if it's changed since it was imported and thus needs to be imported again.
         */
        try
        {
            handleTapeWithTakeOwnershipPending();
        }
        catch ( final RpcException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.DELAYED_OWNERSHIP_FAILURE,
                    ex );
            handleFailure( "Failed to handle tape with take ownership pending.", ex );
        }
    }
    
    
    private LoadedTapeInformation getLoadedTapeInformation()
    {
        try
        {
            final LoadedTapeInformation result = m_tdResource.getLoadedTapeInformation().get( Timeout.LONG );
            m_tapeFailureManagement.resetFailures(
                    m_tape.getId(),
                    m_driveId,
                    TapeFailureType.GET_TAPE_INFORMATION_FAILED);
            return result;
        }
        catch ( final RpcException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.GET_TAPE_INFORMATION_FAILED,
                    ex );
            handleFailure( "Failed to get loaded tape information.", ex );
            return null;
        }
    }
    
    
    private void handleTapeWithTakeOwnershipPending()
    {
        if ( !m_tdResource.hasChangedSinceCheckpoint( 
                m_tape.getLastCheckpoint() ).get( Timeout.LONG ).booleanValue() )
        {
            takeOwnershipOfTapeIfPossible();
            return;
        }

        final String msg = "Tape has changed since it was last imported, so it must be re-imported.";
        TapeTaskUtils.LOG.warn( msg );
        m_serviceManager.getService( TapeService.class ).transistState(
                m_tape, TapeState.FOREIGN );
        m_tapeFailureManagement.registerFailure(
                m_tape.getId(),
                TapeFailureType.REIMPORT_REQUIRED,
                new RuntimeException( msg ) );
    }
    
    
    private void takeOwnershipOfTapeIfPossible()
    {
    	final TapeDriveType driveType =
        		new TapeRM( m_tape, m_serviceManager ).getPartition().getDriveType();
        if ( m_lti.isReadOnly() || !driveType.isWriteSupported( m_lti.getType() ))
        {
            return;
        }
        TapeTaskUtils.LOG.info( "Tape " + m_tape.getId()
                + " can have deferred ownership taken of it as it is no longer write protected." );
        final String checkpoint =
                m_tdResource.takeOwnershipOfTape( m_tape.getId() ).get( Timeout.VERY_LONG );
        m_serviceManager.getService( TapeService.class ).update( 
                m_tape.setLastCheckpoint( checkpoint ).setTakeOwnershipPending( false ),
                Tape.LAST_CHECKPOINT, Tape.TAKE_OWNERSHIP_PENDING );
    }
    
    
    private void verifyQuiescedToCheckpoint()
    {
        try
        {
            if ( m_recordTapeFailure == RestoreExpected.NO && m_tape.isAllowRollback() )
            {
                TapeTaskUtils.LOG.warn("Quiesce appears to have already been initiated and failed for "
                        + m_tapeDescription + ". Will allow rollbacks to occur if necessary.");
            }
            boolean alwaysRollBack = m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() )
                    .getAlwaysRollback();

            final boolean allowRollback = (alwaysRollBack) || (m_recordTapeFailure == RestoreExpected.YES || m_tape.isAllowRollback());
            final String newCheckpoint = m_tdResource.verifyQuiescedToCheckpoint(
                    m_tape.getLastCheckpoint(), allowRollback ).get( Timeout.VERY_LONG );
            if ( null == newCheckpoint )
            {
                if (m_tape.isAllowRollback()) {
                    m_serviceManager.getUpdater(Tape.class).update(
                            m_tape.setAllowRollback(false),
                            Tape.ALLOW_ROLLBACK);
                }
                return;
            }

            if (!allowRollback) {
                //We did not rollback, but we do need to update the checkpoint to the newest equivalent checkpoint
                TapeTaskUtils.LOG.info("Updated checkpoint to equivalent checkpoint for " + m_tapeDescription + ".");
            }
            else
            {
                //We had to update the checkpoint, but we don't know if it's technically a rollback or an update
                TapeTaskUtils.LOG.info( "Checkpoint required an update for " + m_tapeDescription + "." );
            }

            try (final NestableTransaction txn = m_serviceManager.startNestableTransaction()) {
                txn.getService( TapeService.class ).update(
                        m_tape.setLastCheckpoint( newCheckpoint ),
                        Tape.LAST_CHECKPOINT );
                txn.getUpdater(Tape.class).update(
                        m_tape.setAllowRollback(false),
                        Tape.ALLOW_ROLLBACK);
                txn.commitNestableTransaction();
            }
            m_tapeFailureManagement.resetFailures(
                    m_tape.getId(),
                    m_driveId,
                    TapeFailureType.ENCRYPTION_ERROR,
                    TapeFailureType.DATA_CHECKPOINT_FAILURE,
                    TapeFailureType.DATA_CHECKPOINT_MISSING);
        }
        catch ( final RpcException ex )
        {
            handleVerifyQuiescedToCheckpointFailed( ex );
        }
    }
    
    
    private void handleVerifyQuiescedToCheckpointFailed( final RpcException ex )
    {
        final String failure;
        if ( TapeResourceFailureCode.BAD_DRIVE_STATE.toString().equals( 
                ex.getFailureType().getCode() ) )
        {
            failure = 
                    "Cannot verify quiesced to checkpoint " + m_tape.getLastCheckpoint() 
                    + " on " + m_tdResource
                    + " since the tape drive is in a bad state and requires its tape be removed.";
        }
        else if ( TapeResourceFailureCode.ENCRYPTION_ERROR.toString().equals(
                ex.getFailureType().getCode() ) )
        {
            failure =
                    "Cannot verify quiesced to checkpoint " + m_tape.getLastCheckpoint()
                            + " on " + m_tdResource
                            + " due to encryption-related errors.";
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.ENCRYPTION_ERROR,
                    ex );
        }
        else if ( TapeResourceFailureCode.HARDWARE_ERROR.toString().equals(
                ex.getFailureType().getCode() ) )
        {
            failure =
                    "Cannot verify quiesced to checkpoint " + m_tape.getLastCheckpoint()
                            + " on " + m_tdResource
                            + " due to hardware error on drive.";
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.HARDWARE_ERROR,
                    ex );
        }
        else if ( TapeResourceFailureCode.CHECKPOINT_NOT_FOUND.toString().equals(
                ex.getFailureType().getCode() ) )
        {
            failure = "Checkpoint " + m_tape.getLastCheckpoint() 
                      + " could not be found on " + m_tapeDescription + ".";
            m_serviceManager.getService( TapeService.class ).transistState( 
                    m_tape, TapeState.DATA_CHECKPOINT_MISSING );
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.DATA_CHECKPOINT_MISSING,
                    ex );
        }
        else if ( TapeResourceFailureCode.CHECKPOINT_DATA_LOSS.toString().equals(
                ex.getFailureType().getCode() ) )
        {
            failure = "Checkpoint " + m_tape.getLastCheckpoint() + " was the last known checkpoint, but it"
                    + " does not reflect the newest filesystem on tape " + m_tapeDescription + ", and would result in" +
                    " data loss on the tape if we rolled back to it.";
            m_serviceManager.getService( TapeService.class ).transistState(
                    m_tape, TapeState.DATA_CHECKPOINT_FAILURE );
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.DATA_CHECKPOINT_FAILURE,
                    ex );
        }
        else if ( TapeResourceFailureCode.CHECKPOINT_ROLLBACK_TOO_FAR.toString().equals(
                ex.getFailureType().getCode() ) )
        {
            failure = "Failed to roll back to checkpoint " + m_tape.getLastCheckpoint()
                      + " on " + m_tapeDescription + " because the checkpoint is too far back.";
            m_serviceManager.getService( TapeService.class ).transistState(
                    m_tape, TapeState.DATA_CHECKPOINT_FAILURE );
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    TapeFailureType.DATA_CHECKPOINT_FAILURE,
                    ex );
        }
        else
        {
            failure = "Failed to verify quiesced to checkpoint " + m_tape.getLastCheckpoint()
                      + " on " + m_tapeDescription + ".";
            m_tapeFailureManagement.registerFailure(
                    m_tape.getId(),
                    ( m_tape.isWriteProtected() ) ?
                            TapeFailureType.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY
                            : TapeFailureType.DATA_CHECKPOINT_FAILURE,
                            ex );
            if ( RpcProxyException.class.isAssignableFrom( ex.getClass() ) )
            {
                m_serviceManager.getService( TapeService.class ).transistState(
                        m_tape,
                        ( m_tape.isWriteProtected() ) ?
                                TapeState.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY
                                : TapeState.DATA_CHECKPOINT_FAILURE );
            }
        }
        
        handleFailure( failure, ex );
    }
    
    
    private void handleFailure( final String failure, final Throwable t )
    {
        switch ( m_failureHandling )
        {
            case LOG_IT:
                TapeTaskUtils.LOG.warn( failure, t );
                break;
            case THROW_EXCEPTION:
                throw new RuntimeException( failure, t );
            default:
                throw new UnsupportedOperationException( "No code for " + m_failureHandling + ".", t );
        }
    }
    
    
    private final Tape m_tape;
    private final String m_tapeDescription;
    private final TapeDriveResource m_tdResource;
    private final BeansServiceManager m_serviceManager;
    private final RestoreExpected m_recordTapeFailure;
    private final FailureHandling m_failureHandling;
    private final LoadedTapeInformation m_lti;
    private final TapeFailureManagement m_tapeFailureManagement;
    private final UUID m_driveId;
}