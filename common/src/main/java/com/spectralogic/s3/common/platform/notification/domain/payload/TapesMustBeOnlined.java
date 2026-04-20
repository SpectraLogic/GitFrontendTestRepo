/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface TapesMustBeOnlined extends SimpleBeanSafeToProxy
{
    String TAPES_TO_ONLINE = "tapesToOnline";
    
    SetOfTapeBarCodes [] getTapesToOnline();
    
    void setTapesToOnline( final SetOfTapeBarCodes [] value );
}
