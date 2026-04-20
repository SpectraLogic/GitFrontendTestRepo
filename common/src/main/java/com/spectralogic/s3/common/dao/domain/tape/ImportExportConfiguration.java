/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

public enum ImportExportConfiguration
{
    /**
     * The tape partition is properly configured and meets all requirements as specified in 
     * //BlueStorm/mainline/product/documents/specs/Ejected Media Management/Ejected Media Management.docx.
     */
    SUPPORTED,
    
    
    /**
     * The tape partition does not meet one or more requirements as specified in 
     * //BlueStorm/mainline/product/documents/specs/Ejected Media Management/Ejected Media Management.docx.
     * <br><br>
     * 
     * Any tape partition that is not configured in standard EE mode is unsupported per the spec above.  Tape
     * partitions that have an unsupported {@link ImportExportConfiguration} cannot have tapes ejected or
     * onlined within them.
     */
    NOT_SUPPORTED
}
