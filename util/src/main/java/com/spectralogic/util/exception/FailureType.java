/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;

public interface FailureType
{
    public int getHttpResponseCode();
    
    
    public String getCode();
}
