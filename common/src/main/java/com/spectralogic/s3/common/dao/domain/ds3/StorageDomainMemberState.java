/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum StorageDomainMemberState
{
    /**
     * The storage domain member is included normally.
     */
    NORMAL,
    
    /**
     * The storage domain member is in the process of being excluded (data that resides on it is being copied
     * to other members).
     */
    EXCLUSION_IN_PROGRESS,
}
