/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.api;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.util.shutdown.Shutdownable;
import lombok.NonNull;

public interface BlobStore extends Shutdownable
{

    void verify( @NonNull final BlobStoreTaskPriority priority, final UUID persistenceTargetId );
    
    
    Set< BlobStoreTask > getTasks();


    Set<? extends BlobStoreTask> getTasksForJob(final UUID jobId);
    
    
    void refreshEnvironmentNow();

    void taskSchedulingRequired();

}
