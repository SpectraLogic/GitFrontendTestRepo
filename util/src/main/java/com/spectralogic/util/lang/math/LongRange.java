/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.math;

public interface LongRange
{
    boolean overlaps( final LongRange other );
    
    
    long getStart();
    
    
    long getEnd();
    
    
    long getLength();
}
