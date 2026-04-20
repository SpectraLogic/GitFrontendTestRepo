/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface TapeEnvironmentInformation extends SimpleBeanSafeToProxy
{
    String LIBRARIES = "libraries";
    
    @Optional
    TapeLibraryInformation [] getLibraries();
    
    void setLibraries( final TapeLibraryInformation [] value );
}
