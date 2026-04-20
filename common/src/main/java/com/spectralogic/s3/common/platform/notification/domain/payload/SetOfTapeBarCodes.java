/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface SetOfTapeBarCodes extends SimpleBeanSafeToProxy
{
    String TAPE_BAR_CODES = "tapeBarCodes";
    
    String [] getTapeBarCodes();
    
    void setTapeBarCodes( final String [] value );
}
