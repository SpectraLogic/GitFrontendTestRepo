/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface TapeFailureInformation extends SimpleBeanSafeToProxy
{
    String TAPE_ID = "tapeId";
    
    UUID getTapeId();
    
    TapeFailureInformation setTapeId( final UUID value );
    
    
    String FAILURE = "failure";
    
    String getFailure();
    
    TapeFailureInformation setFailure( final String value );
}
