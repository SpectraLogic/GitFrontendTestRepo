/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.api;

import com.spectralogic.s3.dataplanner.backend.api.RunnableBlobStoreTask;

public interface PoolTask extends RunnableBlobStoreTask
{
    /**
     * After this method is invoked, {@link #getPoolId()} should return non-null so that the correct pool can
     * be locked.  If {@link #getPoolId()} returns null, the task cannot be executed and should be retried 
     * later.
     */
    void prepareForExecutionIfPossible();
}
