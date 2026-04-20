package com.spectralogic.s3.dataplanner.backend.tape;

import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.tape.DriveTestResult;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.*;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import org.apache.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class TapeDriveResourceWrapper implements TapeDriveResource {

    public TapeDriveResourceWrapper(
            final TapeDriveResource resource,
            final Callable<Boolean> idleCallback,
            final Long maxIdleTime) {
        m_resource = resource;
        m_idleCallback = idleCallback;
        m_maxIdleTime = maxIdleTime;
        scheduleTimeout();
    }

    private void scheduleTimeout() {
        synchronized (timerLock) {
            if (m_maxIdleTime == null) {
                return;
            }
            try {
                m_timer = new Timer();
                m_timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        boolean idleCallbackSuccessful = false;
                        try {
                            idleCallbackSuccessful = m_idleCallback.call();
                            cancelTimeout();
                        } catch (final Exception e) {
                            cancelTimeout();
                        }
                        if (!idleCallbackSuccessful) {
                            scheduleTimeout();
                        }
                    }
                }, m_maxIdleTime);
            } catch (final Exception e) {
                LOG.warn("Failed to schedule drive idle timer.", e);
            }
        }
    }

    private void cancelTimeout() {
        synchronized (timerLock) {
            try {
                if (m_timer != null) {
                    m_timer.cancel();
                    m_timer = null;
                }
            } catch (final Exception e) {
                LOG.warn("Failed to cancel drive idle timer.", e);
            }
        }
    }

    private <T> RpcFuture<T> wrapFunction(final Supplier<RpcFuture<T>> func) {
        cancelTimeout();
        final RpcFuture<T> retval = func.get();
        scheduleTimeout();
        return retval;
    }

    @Override
    public RpcFuture<LoadedTapeInformation> getLoadedTapeInformation() {
        return wrapFunction(() -> m_resource.getLoadedTapeInformation());
    }

    @Override
    public RpcFuture<String> getLoadedTapeSerialNumber() {
        return wrapFunction(() -> m_resource.getLoadedTapeSerialNumber());
    }

    @Override
    public RpcFuture<FormattedTapeInformation> getFormattedTapeInformation() {
        return wrapFunction(() -> m_resource.getFormattedTapeInformation());
    }

    @Override
    public RpcFuture<BlobIoFailures> writeData(final LtfsFileNamingMode ltfsFileNamingMode, final S3ObjectsIoRequest objectsToWriteToTape) {
        return wrapFunction(() -> m_resource.writeData(ltfsFileNamingMode, objectsToWriteToTape));
    }

    @Override
    public RpcFuture<LtfsFileNamingMode> getLtfsFileNamingMode() {
        return wrapFunction(() -> m_resource.getLtfsFileNamingMode());
    }

    @Override
    public RpcFuture<BlobIoFailures> readData(final S3ObjectsIoRequest objectsToReadIntoCache) {
        return wrapFunction(() -> m_resource.readData(objectsToReadIntoCache));
    }

    @Override
    public RpcFuture<BlobIoFailures> verifyData(final S3ObjectsToVerify objectsToVerify) {
        return wrapFunction(() -> m_resource.verifyData(objectsToVerify));
    }

    @Override
    public RpcFuture<String> openDs3Contents(final boolean includeObjectMetadata, final boolean recursive) {
        return wrapFunction(() -> m_resource.openDs3Contents(includeObjectMetadata, recursive));
    }

    @Override
    public RpcFuture<String> openForeignContents(final String bucketName, final String blobCountMetadataKey, final String creationDateMetadataKey, final long maxBlobSize, final long maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata) {
        return wrapFunction(() -> m_resource.openForeignContents(bucketName, blobCountMetadataKey, creationDateMetadataKey, maxBlobSize, maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata));
    }

    @Override
    public RpcFuture<S3ObjectsOnMedia> readContents(final String handle, final int preferredMaximumNumberOfResultsReturned, final long preferredMaximumTotalBlobLengthInBytesReturned) {
        return wrapFunction(() -> m_resource.readContents(handle, preferredMaximumNumberOfResultsReturned, preferredMaximumTotalBlobLengthInBytesReturned));
    }

    @Override
    public RpcFuture<?> closeContents(final String handle) {
        return wrapFunction(() -> (RpcFuture<String>) m_resource.closeContents(handle));
    }

    @Override
    public RpcFuture<String> quiesce() {
        return wrapFunction(() -> m_resource.quiesce());
    }

    @Override
    public RpcFuture<String> verifyQuiescedToCheckpoint(final String checkpointIdentifier, final boolean allowRollback) {
        return wrapFunction(() -> m_resource.verifyQuiescedToCheckpoint(checkpointIdentifier, allowRollback));
    }

    @Override
    public RpcFuture<String> verifyConsistent() {
        return wrapFunction(() -> m_resource.verifyConsistent());
    }

    @Override
    public RpcFuture<Boolean> hasChangedSinceCheckpoint(final String checkpointIdentifier) {
        return wrapFunction(() -> m_resource.hasChangedSinceCheckpoint(checkpointIdentifier));
    }

    @Override
    public RpcFuture<?> prepareForRemoval() {
        return wrapFunction(() -> (RpcFuture<String>) m_resource.prepareForRemoval());
    }

    @Override
    public RpcFuture<?> format(final boolean characterize, final TapeDriveType density) {
        return wrapFunction(() -> (RpcFuture<String>) m_resource.format(characterize, density));
    }

    @Override
    public RpcFuture<String> inspect() {
        return wrapFunction(() -> m_resource.inspect());
    }

    @Override
    public RpcFuture<String> takeOwnershipOfTape(final UUID tapeId) {
        return wrapFunction(() -> m_resource.takeOwnershipOfTape(tapeId));
    }

    @Override
    public RpcFuture<?> waitForDriveCleaningToComplete() {
        return wrapFunction(() -> (RpcFuture<String>) m_resource.waitForDriveCleaningToComplete());
    }

    @Override
    public RpcFuture<DriveTestResult> driveTestPostB() {
        return wrapFunction(() -> m_resource.driveTestPostB());
    }

    @Override
    public RpcFuture<?> driveDump() {
        return wrapFunction(() -> m_resource.driveDump());

    }

    @Override
    public boolean isServiceable() {
        return m_resource.isServiceable();
    }

    @Override
    public ConcurrentRequestExecutionPolicy getConcurrentRequestExecutionPolicy() {
        return m_resource.getConcurrentRequestExecutionPolicy();
    }

    @Override
    public RpcFuture<?> ping() {
        return wrapFunction(() -> (RpcFuture<String>) m_resource.ping());
    }

    private final TapeDriveResource m_resource;
    private final Callable<Boolean> m_idleCallback;
    private final Long m_maxIdleTime;
    private Timer m_timer;
    private final Object timerLock = new Object();
    private final static Logger LOG = Logger.getLogger(TapeDriveResourceWrapper.class);
}
