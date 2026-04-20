/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.api;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;

public interface RunnableBlobStoreTask extends BlobStoreTask, Runnable
{
    /**
     * Called if a task has been prepared to start, but after preparations, a failure resulted in the task
     * not being able to run at this time.
     * @param ex
     */
    void executionFailed(RuntimeException ex);
    
    
    /**
     * A task will eventually complete or go back to a ready state (if it asked to be re-executed).  When this
     * occurs, either another task should be assigned to use the newly-freed tape drive resource, or if this 
     * task went back to a ready state to be re-executed, this task needs to be re-executed.  <br><br>
     * 
     * Note: The most common reason that a task will ask to be re-executed is if it decides it needs to use a 
     * tape other than the one it said it would use originally.  This is because a task is not allowed to 
     * change the tape loaded in the tape drive directly.  Furthermore, changing the tape in a tape drive can 
     * take several minutes, especially if there are multiple tape move operations queued up.
     */
    void addSchedulingListener( final BlobStoreTaskSchedulingListener listener );
    
    
    /**
     * Called if a task has been permanently dequeued from execution.
     */
    void dequeued();
}
