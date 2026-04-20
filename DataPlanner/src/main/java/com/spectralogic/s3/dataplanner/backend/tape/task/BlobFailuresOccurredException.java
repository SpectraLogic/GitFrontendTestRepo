package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;

public final class BlobFailuresOccurredException extends RuntimeException {
    BlobFailuresOccurredException(final BlobIoFailure[] failures) {
        super(failures.length + " blobs failed to be written to tape.");
        m_retryable = extractRetryable(failures);
    }


    private boolean extractRetryable(final BlobIoFailure[] failures) {
        for (final BlobIoFailure failure : failures) {
            if (BlobIoFailureType.OUT_OF_SPACE == failure.getFailure()) {
                return false;
            }
        }

        return true;
    }


    public boolean isRetryable() {
        return m_retryable;
    }


    private final boolean m_retryable;
} // end inner class def
