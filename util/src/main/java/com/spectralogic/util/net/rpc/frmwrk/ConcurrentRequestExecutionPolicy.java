/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

public enum ConcurrentRequestExecutionPolicy
{
    /**
     * Multiple requests can be made in parallel
     */
    CONCURRENT,
    
    /**
     * Only one request can be made at a time
     */
    SERIALIZED
}
