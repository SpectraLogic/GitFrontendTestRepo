/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobIoFailures extends SimpleBeanSafeToProxy
{
    String FAILURES = "failures";
    
    @Optional
    BlobIoFailure [] getFailures();
    
    BlobIoFailures setFailures( final BlobIoFailure [] value );
}
