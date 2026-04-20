/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

public enum BlobStoreTaskState
{
    /**
     * Temporarily not ready and cannot be executed at this time
     */
    NOT_READY,
    
    
    /**
     * Ready to be executed once all required resources become available
     */
    READY,
    
    
    /**
     * The task has been scheduled for immediate execution, with all required resources being locked and 
     * provisioned, so that execution may begin imminently
     */
    PENDING_EXECUTION,
    
    
    /**
     * The task is in the process of being executed
     */
    IN_PROGRESS,
    
    
    /**
     * The task has completed and its resources may be unlocked / released and re-used for another task
     */
    COMPLETED,
}
