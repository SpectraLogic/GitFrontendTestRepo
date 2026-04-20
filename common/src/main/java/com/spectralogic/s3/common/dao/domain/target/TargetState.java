/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

public enum TargetState
{
    /**
     * We can communicate with the target.
     */
    ONLINE,
    
    
    /**
     * We cannot communicate with the target.
     */
    OFFLINE
}
