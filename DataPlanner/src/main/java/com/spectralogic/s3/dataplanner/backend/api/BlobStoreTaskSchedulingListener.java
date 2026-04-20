/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.api;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;

public interface BlobStoreTaskSchedulingListener
{
    /**
     * @param task - The task that triggered task scheduling required (task may have completed or failed);
     * can be null, indicating that the task scheduling required event is not related to a task
     */
    void taskSchedulingRequired( final BlobStoreTask task );
}
