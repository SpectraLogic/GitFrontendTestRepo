/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum UnavailableMediaUsagePolicy
{
    /**
     * Unavailable media can be used and shall be prioritized as if the media were online.
     */
    ALLOW,
    
    
    /**
     * Unavailable media can be used, but only if no available media can be used.
     */
    DISCOURAGED,
    
    
    /**
     * Unavailable media shall not be used.
     */
    DISALLOW,
}
