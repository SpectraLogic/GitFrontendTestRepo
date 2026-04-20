package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;

public final class WriteSourceFailedException extends RuntimeException {
    WriteSourceFailedException(final BlobIoFailure[] failures) {
        super(failures.length + " blobs failed to be read from their source (pool) during write to tape.");
    }
}
