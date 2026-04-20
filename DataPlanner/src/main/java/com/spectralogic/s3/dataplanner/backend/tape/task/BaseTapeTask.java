/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import org.apache.commons.io.FileUtils;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeService.TapeAccessType;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.FormattedTapeInformation;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskSchedulingListener;
import com.spectralogic.s3.dataplanner.backend.frmwrk.BaseTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.render.BytesRenderer;

abstract class BaseTapeTask extends BaseTask implements TapeTask
{
    protected BaseTapeTask(final BlobStoreTaskPriority priority, final UUID tapeId, TapeFailureManagement tapeFailureManagement, final BeansServiceManager serviceManager)
    {
        super( priority, serviceManager);
        m_defaultTapeId = tapeId;
        m_tapeFailureManagement = tapeFailureManagement;
        addSchedulingListener( new CleanupInternalStateUponTaskExecutionClosureWorker() );
    }


    // This constructor is used for testing when a larger max retries is needed in order for tests to run in a
    // timely fashion.
    protected BaseTapeTask(final BlobStoreTaskPriority priority, final UUID tapeId, TapeFailureManagement tapeFailureManagement, final BeansServiceManager serviceManager, final int maxRetriesBeforeSuspensionRequired)
    {
        super( priority, serviceManager, maxRetriesBeforeSuspensionRequired );
        m_defaultTapeId = tapeId;
        m_tapeFailureManagement = tapeFailureManagement;
        addSchedulingListener( new CleanupInternalStateUponTaskExecutionClosureWorker() );
    }
    
    
    private final class CleanupInternalStateUponTaskExecutionClosureWorker
        implements BlobStoreTaskSchedulingListener
    {
        public void taskSchedulingRequired( final BlobStoreTask task )
        {
            m_driveResource = null;
            m_driveId = null;
        }
    } // end inner class def
    
    public void prepareForExecutionIfPossible(
            final TapeDriveResource tapeDriveResource,
            final TapeAvailability tapeAvailability)
    {
        if (BlobStoreTaskState.READY != getRawState()) {
            throw new IllegalStateException(
                    "Cannot prepare for start when " + this + " is in state " + getState() + ".");
        }
        String failureToVerifyAvailability = null;
        try {
            LOG.info("Preparing to execute " + toString() + "...");
            failureToVerifyAvailability = tapeAvailability.verifyAvailable(getTapeId());
            m_driveId = tapeAvailability.getDriveId();
            m_tapePartitionId = tapeAvailability.getTapePartitionId();
            m_driveResource = tapeDriveResource;
        } catch( final RuntimeException e) {
            invalidateTaskAndThrow( e );
        }
        if (null != failureToVerifyAvailability) {
            throw new RuntimeException("Selected tape (" + getTapeId() + ") was unavailable since "
                    + failureToVerifyAvailability + ".");
        }
        preparedForExecution();
        LOG.info("Prepared to execute " + toString() + ".  Will use tape: " + getTapeId());

    }
    
    
    @Override
    protected void performPreRunValidations()
    {
        verifyTapeInDrive( new DefaultTapeInDriveVerifier(this, false ) );
    }
    
    
    interface TapeInDriveVerifier extends Runnable
    {
        // marker interface
    }
    
    
    final protected void verifyTapeInDrive( final TapeInDriveVerifier verifier )
    {
        final TapeService tapeService = getServiceManager().getService( TapeService.class );
        final Tape tape = getTape();
        try
        {
            verifier.run();
            tapeService.updateDates( tape.getId(), TapeAccessType.ACCESSED );
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.GET_TAPE_INFORMATION_FAILED);
        }
        catch ( final RpcException ex )
        {
            tapeService.transistState( tape, TapeState.UNKNOWN );
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.GET_TAPE_INFORMATION_FAILED,
                    ex );
            throw ex;
        }
    }
    
    
    final protected void verifyTapeIsWritable()
    {
        if ( getTape().isWriteProtected() )
        {
            throw new IllegalStateException( "Cannot write to tape since it's write protected." );
        }
    }



    
    
    final protected void updateTapeDateLastModified()
    {
        getServiceManager().getService( TapeService.class ).updateDates(
                getTapeId(),
                TapeAccessType.MODIFIED );
    }
    
    
    /**
     * @return TRUE if the tape extended information was successfully updated
     */
    final protected boolean updateTapeExtendedInformation()
    {
        final FormattedTapeInformation tapeInformation;
        try
        {
            tapeInformation = getDriveResource().getFormattedTapeInformation().get( Timeout.VERY_LONG );
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.GET_TAPE_INFORMATION_FAILED);
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to get tape information for tape " + getTapeId()
                      + ", so can't update its extended information.", ex );
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    TapeFailureType.GET_TAPE_INFORMATION_FAILED,
                    ex );
            return false;
        }
        
        final Tape tape = getTape();
        if ( null != tape.getAvailableRawCapacity() 
                && tape.getAvailableRawCapacity() < tapeInformation.getAvailableRawCapacity() )
        {
            final BytesRenderer renderer = new BytesRenderer();
            final long bytesOld = tape.getAvailableRawCapacity();
            final long bytesNew = tapeInformation.getAvailableRawCapacity();
            LOG.warn( "Tape " + tape.getId() + " (" + tape.getBarCode() 
                      + ") increased its available raw capacity from " 
                      + renderer.render( bytesOld ) + " to " + renderer.render( bytesNew )
                      + " (" + bytesOld + " --> " + bytesNew + ")." );
        }
        
        tape.setAvailableRawCapacity( tapeInformation.getAvailableRawCapacity() );
        tape.setTotalRawCapacity( tapeInformation.getTotalRawCapacity() );
        tape.setCharacterizationVer( tapeInformation.getCharacterizationVer() );
        getServiceManager().getService( TapeService.class ).update(
                tape,
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY, Tape.CHARACTERIZATION_VER );
        return true;
    }
    
    
    public final UUID getTapePartitionId()
    {
        return m_tapePartitionId;
    }
    
    
    public final UUID getDriveId()
    {
        return m_driveId;
    }
    
    
    public final TapeDriveResource getDriveResource()
    {
        return m_driveResource;
    }
    
    
    public UUID getTapeId()
    {
        return m_defaultTapeId;
    }
    
    
    public final UUID getPoolId()
    {
        return null;
    }
    
    
    public final String getTargetType()
    {
        return null;
    }
    
    
    public final UUID getTargetId()
    {
        return null;
    }
    
    
    final protected Tape getTape()
    {
        return getServiceManager().getRetriever( Tape.class ).attain( getTapeId() );
    }
    
    
    final protected long getPageSegmentLength()
    {
        if ( null == getTape().getTotalRawCapacity() )
        {
            return DEFAULT_PAGE_SEGMENT_LENGTH;
        }
        return getTape().getTotalRawCapacity() / 10;
    }
    

    private volatile UUID m_driveId;
    private volatile UUID m_tapePartitionId;
    private volatile TapeDriveResource m_driveResource;
    
    protected final UUID m_defaultTapeId;
    protected final TapeFailureManagement m_tapeFailureManagement;
    private final static long DEFAULT_PAGE_SEGMENT_LENGTH = 150 * FileUtils.ONE_GB;
}
