/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

public enum ElementAddressType
{
    /**
     * The element address is a tape storage slot
     */
    STORAGE,
    
    /**
     * The element address is a tape drive
     */
    TAPE_DRIVE,
    
    /**
     * Spectra Logic libraries will never report that the robot has a tape
     */
    ROBOT,
    
    /**
     * The element address is a tape slot dedicated to import/export operations
     */
    IMPORT_EXPORT
}
