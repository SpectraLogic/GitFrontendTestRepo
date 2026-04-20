/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape.domain;


/**
 * Information about a formatted tape loaded in a tape drive that requires the tape to be LTFS-mounted.
 */
public interface FormattedTapeInformation extends LoadedTapeInformation
{
    String TOTAL_RAW_CAPACITY = "totalRawCapacity";
    
    long getTotalRawCapacity();
    
    void setTotalRawCapacity( final long value );
    
    
    String AVAILABLE_RAW_CAPACITY = "availableRawCapacity";

    long getAvailableRawCapacity();
    
    void setAvailableRawCapacity( final long value );
}
