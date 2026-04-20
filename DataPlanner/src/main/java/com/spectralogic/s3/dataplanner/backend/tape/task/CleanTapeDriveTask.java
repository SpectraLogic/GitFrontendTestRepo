/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CleanTapeDriveTask extends BaseTapeTask implements StaticTapeTask
{
    public CleanTapeDriveTask(final TapeDrive drive,
                              final UUID tapeId,
                              final TapeFailureManagement tapeFailureManagement,
                              BeansServiceManager serviceManager)
    {
        super( BlobStoreTaskPriority.CRITICAL, tapeId, tapeFailureManagement, serviceManager );
        m_drive = drive;
        m_tapeFailureManagement = tapeFailureManagement;
        Validations.verifyNotNull( "Drive", m_drive );
    }


    @Override
    public boolean canUseDrive(final UUID tapeDriveId ) {
        return tapeDriveId != null && tapeDriveId.equals(getTapeDriveToClean());
    }

    
    @Override
    public void performPreRunValidations()
    {
        // empty
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        try
        {
            getDriveResource().waitForDriveCleaningToComplete().get( Timeout.VERY_LONG );
            getServiceManager().getService( TapeDriveService.class ).update( 
                    m_drive.setLastCleaned( new Date() ), TapeDrive.LAST_CLEANED );
            m_tapeFailureManagement.resetFailures(getTapeId(),
                    m_drive.getId(),
                    TapeFailureType.DRIVE_CLEAN_FAILED);
        }
        catch ( final RpcProxyException ex )
        {
            if ( GenericFailure.CONFLICT.getHttpResponseCode() == ex.getFailureType().getHttpResponseCode() )
            {
                getServiceManager().getService( TapeService.class ).transistState(
                        getTape(), TapeState.BAD );
            }
            if (ex.getFailureType().getHttpResponseCode() == 409)
            {
                m_tapeFailureManagement.registerFailure(
                        getTapeId(),
                        TapeFailureType.CLEANING_TAPE_EXPIRED,
                        ex );
            }
            else
            {
                m_tapeFailureManagement.registerFailure(
                        getTapeId(),
                        TapeFailureType.DRIVE_CLEAN_FAILED,
                        ex );
            }
        }
        
        return BlobStoreTaskState.COMPLETED;
    }


    @Override
    public boolean allowMultiplePerTape() {
        return true;
    }

    
    public String getDescription()
    {
        return "Clean Drive " + m_drive.getId() + " Using Cleaning Tape " + getTapeId();
    }
    
    
    public UUID getTapeDriveToClean()
    {
        return m_drive.getId();
    }


    private final TapeDrive m_drive;
    private final TapeFailureManagement m_tapeFailureManagement;
}
