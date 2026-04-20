/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api;

import java.util.UUID;

public interface TapeMoveListener
{
    void validationCompleted( final UUID tapeId, final RuntimeException failure );
    
    
    void moveSucceeded( final UUID tapeId );
    
    
    void moveFailed( final UUID tapeId );
}
