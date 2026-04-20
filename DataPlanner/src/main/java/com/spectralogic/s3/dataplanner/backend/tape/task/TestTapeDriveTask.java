/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.util.UUID;

import static com.spectralogic.s3.common.dao.domain.tape.DriveTestResult.*;

public final class TestTapeDriveTask extends BaseTapeTask implements StaticTapeTask
{
    public TestTapeDriveTask(final TapeDrive drive,
                             final UUID tapeId,
                             final TapeFailureManagement tapeFailureManagement,
                             final BeansServiceManager serviceManager)
    {
        super( BlobStoreTaskPriority.CRITICAL, tapeId, tapeFailureManagement, serviceManager );
        m_drive = drive;
        m_tapeFailureManagement = tapeFailureManagement;
        Validations.verifyNotNull( "Drive", m_drive );
    }


    @Override
    public boolean canUseDrive(final UUID tapeDriveId ) {
        return tapeDriveId != null && tapeDriveId.equals(getTapeDriveToTest());
    }


    @Override
    public void prepareForExecutionIfPossible(
            final TapeDriveResource tapeDriveResource,
            final TapeAvailability tapeAvailability )
    {
        validateTapeEligibility();
        super.prepareForExecutionIfPossible(tapeDriveResource, tapeAvailability);
    }


    @Override
    protected void performPreRunValidations()
    {
        validateTapeEligibility();
    }

    private void validateTapeEligibility() {
        final Tape tape = m_serviceManager.getRetriever(Tape.class).attain(getTapeId());
        if (tape.getState() != TapeState.NORMAL
                || !m_drive.getPartitionId().equals(tape.getPartitionId())
                || !tape.getRole().equals(TapeRole.TEST)
                || tape.isAssignedToStorageDomain()
                || tape.getStorageDomainMemberId() != null) {
            invalidateTaskAndThrow( new IllegalStateException("Tape " + tape.getId() + " is no longer available for testing.") );
        }
    }


    @Override
    protected BlobStoreTaskState runInternal()
    {
        try
        {
            final DriveTestResult result = getDriveResource().driveTestPostB().get( Timeout.VERY_LONG );
            if (result == SUCCESS) {
                m_tapeFailureManagement.resetFailures( getTapeId(),
                        m_drive.getId(),
                        TapeFailureType.DRIVE_TEST_FAILED_ALL_WRITES_TOO_SLOW,
                        TapeFailureType.DRIVE_TEST_FAILED_FORWARD_WRITES_TOO_SLOW,
                        TapeFailureType.DRIVE_TEST_FAILED_REVERSE_WRITES_TOO_SLOW,
                        TapeFailureType.DRIVE_TEST_FAILED );
            } else {
                final TapeFailureType resultType;
                if (result == FAILED_ALL_WRITES_TOO_SLOW) {
                    resultType = TapeFailureType.DRIVE_TEST_FAILED_ALL_WRITES_TOO_SLOW;
                } else if (result == FAILED_FORWARD_WRITES_TOO_SLOW) {
                    resultType = TapeFailureType.DRIVE_TEST_FAILED_FORWARD_WRITES_TOO_SLOW;
                } else if (result == FAILED_REVERSE_WRITES_TOO_SLOW) {
                    resultType = TapeFailureType.DRIVE_TEST_FAILED_REVERSE_WRITES_TOO_SLOW;
                } else {
                    resultType = TapeFailureType.DRIVE_TEST_FAILED;
                }
                m_tapeFailureManagement.registerFailure(
                        getTapeId(),
                        resultType,
                        new RuntimeException("Drive " + getTapeDriveToTest() + " was tested with tape " + getTapeId()
                                + " and failed with: " + resultType) );
            }


        }
        catch ( final RpcProxyException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.DRIVE_TEST_FAILED,
                    ex );
        }
        
        return BlobStoreTaskState.COMPLETED;
    }


    @Override
    public boolean allowMultiplePerTape() {
        return true;
    }

    
    public String getDescription()
    {
        return "Test Drive " + m_drive.getId() + " Using Test Tape " + getTapeId();
    }
    
    
    public UUID getTapeDriveToTest()
    {
        return m_drive.getId();
    }

    
    private final TapeDrive m_drive;
    private final TapeFailureManagement m_tapeFailureManagement;
}
