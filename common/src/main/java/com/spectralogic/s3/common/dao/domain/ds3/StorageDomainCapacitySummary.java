/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface StorageDomainCapacitySummary extends SimpleBeanSafeToProxy
{
    String PHYSICAL_USED = "physicalUsed";
    
    /**
     * @return the physical capacity used (in bytes), which could vary significantly from the logical used due
     * to the following possibilities: <br><br>
     * 
     * 1) An object may not have been written to tape yet <br>
     * 2) An object may consume less physical space on tape than its logical size due to compression <br>
     * 3) An object may be stored on multiple tapes <br>
     * 4) An object may have been deleted logically, but still exist on tape <br>
     * 5) A tape may have bad sectors, causing an object to consume more physical space on tape than its 
     *    logical size <br>
     */
    long getPhysicalUsed();
    
    void setPhysicalUsed( final long value );
    
    
    String PHYSICAL_ALLOCATED = "physicalAllocated";
    
    /**
     * @return the physical capacity allocated to this bucket (in bytes)
     */
    long getPhysicalAllocated();
    
    void setPhysicalAllocated( final long value );
    
    
    String PHYSICAL_FREE = "physicalFree";
    
    /**
     * @return the estimated physical capacity allocated <b>and available now</b> (e.g. online in a normal 
     * state) for object data (in bytes)  <br><br>
     * 
     * Note that {@link #PHYSICAL_FREE} = {@link #PHYSICAL_ALLOCATED} - {@link #PHYSICAL_USED} iff all media
     * capacity being reported on is available <b>now</b>.
     */
    long getPhysicalFree();
    
    void setPhysicalFree( final long value );
}
