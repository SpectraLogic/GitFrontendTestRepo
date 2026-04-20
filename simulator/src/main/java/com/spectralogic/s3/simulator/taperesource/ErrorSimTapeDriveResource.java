package com.spectralogic.s3.simulator.taperesource;

import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.tape.DriveTestResult;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.domain.*;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;

import java.util.UUID;

public class ErrorSimTapeDriveResource  extends BaseRpcResource implements TapeDriveResource {
    @Override
    public RpcFuture<LoadedTapeInformation> getLoadedTapeInformation() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> getLoadedTapeSerialNumber() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<FormattedTapeInformation> getFormattedTapeInformation() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<BlobIoFailures> writeData(LtfsFileNamingMode ltfsFileNamingMode, S3ObjectsIoRequest objectsToWriteToTape) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<LtfsFileNamingMode> getLtfsFileNamingMode() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<BlobIoFailures> readData(S3ObjectsIoRequest objectsToReadIntoCache) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<BlobIoFailures> verifyData(S3ObjectsToVerify objectsToVerify) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> openDs3Contents(boolean includeObjectMetadata, boolean recursive) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> openForeignContents(String bucketName, String blobCountMetadataKey, String creationDateMetadataKey, long maxBlobSize, long maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<S3ObjectsOnMedia> readContents(String handle, int preferredMaximumNumberOfResultsReturned, long preferredMaximumTotalBlobLengthInBytesReturned) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<?> closeContents(String handle) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> quiesce() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> verifyQuiescedToCheckpoint(String checkpointIdentifier, boolean allowRollback) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> verifyConsistent() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<Boolean> hasChangedSinceCheckpoint(String checkpointIdentifier) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<?> prepareForRemoval() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<?> format(boolean characterize, TapeDriveType density) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> inspect() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<String> takeOwnershipOfTape(UUID tapeId) {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<?> waitForDriveCleaningToComplete() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<DriveTestResult> driveTestPostB() {
        sleepForever();
        return null;
    }

    @Override
    public RpcFuture<?> driveDump() {
        sleepForever();
        return null;
    }

    private void sleepForever() {
        while (true) {
            try {
                Thread.sleep(60 * 60 * 1000 );
            } catch (InterruptedException e)
            {

            }
        }
    }
}
