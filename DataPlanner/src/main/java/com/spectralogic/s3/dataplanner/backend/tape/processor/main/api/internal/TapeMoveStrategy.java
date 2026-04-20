/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeMoveListener;

import java.util.List;

public interface TapeMoveStrategy
{
    /**
     * @return the destination slot number for the tape move
     */
    int getDest(
            final int src, 
            final Tape tape,
            final TapeEnvironmentManager tapeEnvironmentManager );
    
    
    void moveSucceeded();
    
    
    void moveFailed( final Tape tape );
    
    
    void addListener( final TapeMoveListener listener );


    List<TapeDrive> getAssociatedDrives();
}
