/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

/**
 * The write optimization determines the level of concurrency a job will be permitted to operate under.
 * <br><br>
 * 
 * Write optimizations are ordered from best capacity utilization per physical data store and minimal impact 
 * to other jobs in the system to best performance, so each optimization is guaranteed to perform at least as 
 * well as the previous one i.e. is guaranteed to have at least as much concurrency as the previous one.
 */
public enum WriteOptimization
{
    /**
     * Maximize concurrent use of available physical data stores to improve performance provided that when
     * the work is all done, that the physical data stores were used efficiently wrt capacity utilization.
     * <br><br>
     * 
     * Assumes that the data cannot be further compressed by the physical data stores.  If data can be further
     * compressed, physical data stores may not be used as efficiently wrt capacity utilization as they could
     * have been.
     */
    CAPACITY,
    
    
    /**
     * Maximize concurrent use of available physical data stores to maximize performance.
     */
    PERFORMANCE,
}
