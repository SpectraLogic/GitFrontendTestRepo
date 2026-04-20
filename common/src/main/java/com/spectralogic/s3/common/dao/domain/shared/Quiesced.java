/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

public enum Quiesced
{
    /**
     * Not quiesced (we can perform operations).
     */
    NO,
    
    
    /**
     * In the process of being quiesced.
     */
    PENDING,
    
    
    /**
     * Quiesced.  No operations are permitted to performed until the quiesced state changes to another value.
     */
    YES
}
