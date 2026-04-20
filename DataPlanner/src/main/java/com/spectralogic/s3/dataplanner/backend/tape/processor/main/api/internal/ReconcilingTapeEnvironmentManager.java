package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal;

import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;

public interface ReconcilingTapeEnvironmentManager extends TapeEnvironmentManager{

    /**
     * Reconciles the tape environment passed in with the current tape environment.  Either the reconciliation
     * succeeds and all state changes are committed or the reconciliation fails and no state changes are
     * committed.
     */
    void reconcileWith( final TapeEnvironmentInformation tapeEnvironment );
}
