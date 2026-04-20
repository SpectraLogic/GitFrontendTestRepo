/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;

public interface DetailedTape extends Tape
{
    String MOST_RECENT_FAILURE = "mostRecentFailure";
    
    TapeFailure getMostRecentFailure();
    
    void setMostRecentFailure( final TapeFailure value );
}
