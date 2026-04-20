/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum JobChunkBlobStoreState
{
    /**
     * The data store has not begun processing the chunk yet.
     */
    PENDING,
    
    /**
     * The chunk is being read / written by the data store.
     */
    IN_PROGRESS,
    
    /**
     * The chunk has been completely read / written by the data store.
     */
    COMPLETED
}
