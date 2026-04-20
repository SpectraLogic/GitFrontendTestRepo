/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

/**
 * The level of isolation of data to enforce.
 */
public enum DataIsolationLevel
{
    /**
     * Data shall be isolated according to standard isolation requirements (such as those enforce by 
     * {@link StorageDomain}s).
     */
    STANDARD,
    
    
    /**
     * Data from different buckets shall not be mixed on the same physical storage media.  This may result
     * in significant space waste.
     */
    BUCKET_ISOLATED,
}
