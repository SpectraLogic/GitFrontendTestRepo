/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface TapeFailuresInformation extends SimpleBeanSafeToProxy
{
    String FAILURES = "failures";
    
    @Optional
    TapeFailureInformation [] getFailures();
    
    TapeFailuresInformation setFailures( final TapeFailureInformation [] value );
}
