/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;


public enum TapePartitionState
{
    /**
     * The partition is online and is allowed to be used by this application.  All tape drives and tapes
     * within an online partition may be used.
     */
    ONLINE,
    
    
    /**
     * The partition is offline and cannot be used until it comes back online.
     */
    OFFLINE,
    
    
    /**
     * The partition is in error and cannot be used until it comes back online.
     */
    ERROR
}
