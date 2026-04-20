/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io.lang;

import java.util.List;

import com.spectralogic.util.lang.math.LongRange;


/**
 * Represents and encapsulates multiple ranges of bytes, expressed as {@link LongRange}s.
 */
public interface ByteRanges
{
    /**
     * @return the ranges that makes up this {@link ByteRanges}  <br><br>
     * 
     * No range is allowed to overlap with any other range.  <br>
     * Ranges will be sorted from the lowest range to the highest range.  <br>
     */
    public List< LongRange > getRanges();
    
    
    /**
     * @return the smallest range that includes all ranges returned by {@link #getRanges}
     */
    public LongRange getFullRequiredRange();
    
    
    /**
     * @return the sum of all range lengths in the ranges returned by {@link #getRanges}
     */
    public long getAggregateLength();
    
    
    /**
     * @param byteOffset The number of bytes by which to shift the byte range forward.
     * @return The shifted byte ranges instance.
     */
    public ByteRanges shift( final long byteOffset );
}