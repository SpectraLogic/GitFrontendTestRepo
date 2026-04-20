/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.planner;

public enum CacheEntryState
{
    /**
     * Cache capacity has been allocated for this element, but the element has not been completely written to
     * cache yet
     */
    ALLOCATED,
    
    
    /**
     * The object has been written to in its entirety and may be serviced from cache
     */
    IN_CACHE,

    /**
     * The file is still in cache but is pending asynchronous deletion
     */
    PENDING_DELETE
}
