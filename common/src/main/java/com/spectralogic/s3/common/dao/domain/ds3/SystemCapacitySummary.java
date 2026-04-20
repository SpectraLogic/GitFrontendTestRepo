/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;


public interface SystemCapacitySummary extends StorageDomainCapacitySummary
{
    String PHYSICAL_AVAILABLE = "physicalAvailable";
    
    /**
     * @return capacity (in bytes) that has not been allocated yet <b>and is available now</b> (e.g. online 
     * in a normal state)
     */
    long getPhysicalAvailable();
    
    void setPhysicalAvailable( final long value );
}
