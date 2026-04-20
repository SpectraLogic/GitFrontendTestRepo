/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;

public interface TapeTaskDequeuedListener
{
    void taskDequeued( final TapeTask task );
}
