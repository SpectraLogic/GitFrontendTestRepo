/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;


public enum TapeDriveState
{
    /**
     * The drive is offline and cannot be used.
     */
    OFFLINE,
    
    
    /**
     * There is no reason why we can't use this tape drive.
     */
    NORMAL,
    
    
    /**
     * We have encountered enough problems using the tape drive that we consider it to be in error and should
     * not use it.
     */
    ERROR,
    
    
    /**
     * A partition must only have tape drives of a single type, even though multiple types of media may be
     * present in the same partition.  If there is a mix of tape drives, only the latest generation drives
     * will be used.
     */
    NOT_COMPATIBLE_IN_PARTITION_DUE_TO_NEWER_TAPE_DRIVES,
}
