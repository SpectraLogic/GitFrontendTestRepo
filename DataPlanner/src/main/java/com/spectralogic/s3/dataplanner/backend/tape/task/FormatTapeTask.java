/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class FormatTapeTask extends BaseTapeTask implements StaticTapeTask
{
    public FormatTapeTask(final BlobStoreTaskPriority priority, final UUID tapeId, TapeFailureManagement tapeFailureManagement, final BeansServiceManager serviceManager)
    {
        this( priority, tapeId, false, tapeFailureManagement, serviceManager );
    }


    public FormatTapeTask(final BlobStoreTaskPriority priority, final UUID tapeId, final boolean characterize, TapeFailureManagement tapeFailureManagement, final BeansServiceManager serviceManager)
    {
        super( priority, tapeId, tapeFailureManagement, serviceManager );
        m_characterize = characterize;
    }
    
    
    @Override
    protected void performPreRunValidations()
    {
        verifyTapeInDrive( new MinimalTapeInDriveVerifier(this) );
    }
    
    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        updateTapeDateLastModified();
        getServiceManager().getService( BlobTapeService.class ).reclaimTape( 
                "tape formatting", getTapeId() );
        
        final Tape tape = getTape();
        final TapeDensityDirective tapeDensityDirective =
                getServiceManager().getRetriever( TapeDensityDirective.class ).retrieve( Require.all( 
                        Require.beanPropertyEquals(
                                TapeDensityDirective.TAPE_TYPE, tape.getType() ),
                        Require.beanPropertyEquals( 
                                TapeDensityDirective.PARTITION_ID, tape.getPartitionId() ) ) );
        final String checkpoint;
        try
        {
            getDriveResource().format( m_characterize,
                    ( null == tapeDensityDirective ) ? null : tapeDensityDirective.getDensity() )
                    .get( Timeout.VERY_LONG );
            checkpoint = getDriveResource().takeOwnershipOfTape( getTapeId() ).get( Timeout.VERY_LONG );
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.FORMAT_FAILED);
        }
        catch ( final RpcException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    tape.getId(),
                    TapeFailureType.FORMAT_FAILED,
                    ex );
            // If the tape has been marked bad due to the failure registration, then stop trying to format it.
            if (TapeState.BAD == getTape().getState())
            {
                return BlobStoreTaskState.COMPLETED;
            }
            throw ex;
        }
        
        if ( !updateTapeExtendedInformation() )
        {
            throw new RuntimeException( "Failed to update tape extended information after formatting it." );
        }
        
        final BeansServiceManager transaction = getServiceManager().startTransaction();
        try
        {
            final TapeService tapeService = transaction.getService( TapeService.class );
            tapeService.transistState( getTape(), TapeState.NORMAL );
            tapeService.update(
                    getTape().setDescriptionForIdentification( 
                            TapeTaskUtils.inspect( 
                                    getTape(),
                                    getDriveId(),
                                    getDriveResource(), 
                                    m_tapeFailureManagement ) )
                            .setFullOfData( false ).setTakeOwnershipPending( false )
                            .setLastCheckpoint( checkpoint ), 
                            Tape.DESCRIPTION_FOR_IDENTIFICATION,
                            Tape.FULL_OF_DATA,
                            Tape.TAKE_OWNERSHIP_PENDING,
                            Tape.LAST_CHECKPOINT );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        getServiceManager().getService( TapeFailureService.class ).deleteAll( getTapeId() );
        inspect();
        
        return BlobStoreTaskState.COMPLETED;
    }
    
    
    @Override
    protected void handlePreparedForExecution()
    {
        final Tape tape = getTape();
        if ( TapeState.FORMAT_PENDING != tape.getState() )
        {
            invalidateTaskAndThrow( "Tape is in state " + tape.getState() );
        }
        
        getTapeService().transistState( tape, TapeState.FORMAT_IN_PROGRESS );
    }
    
    
    @Override
    protected void handleExecutionFailed()
    {
        getTapeService().transistState( getTape(), TapeState.FORMAT_PENDING );
    }
    
    
    private TapeService getTapeService()
    {
        return getServiceManager().getService( TapeService.class );
    }


    public String getDescription()
    {
        return "Format Tape " + m_defaultTapeId;
    }
    
    
    private void inspect()
    {
        final InspectTapeTask inspectTask = new InspectTapeTask( getPriority(), getTapeId(), m_tapeFailureManagement, m_serviceManager );
        LOG.info( "Tape has been formatted.  Will inspect it using task #" + inspectTask.getId() + "." );
        inspectTask.prepareForExecutionIfPossible(
                getDriveResource(),
                new TapeAvailabilityImpl( getTapePartitionId(), getDriveId(), getTapeId() ) );
        inspectTask.run();
    }


    @Override
    public boolean allowMultiplePerTape() {
        return false;
    }

    final boolean m_characterize;
}
