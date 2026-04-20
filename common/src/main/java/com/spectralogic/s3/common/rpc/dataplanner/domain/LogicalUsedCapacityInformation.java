/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface LogicalUsedCapacityInformation extends SimpleBeanSafeToProxy
{
    String CAPACITIES = "capacities";
    
    /**
     * @return capacities, ordered according to the order of the requests originally made <br><br>
     * 
     * Note: If the capacity for a particular request is unknown (e.g. it is in the process of being
     * determined), -1 will be returned.  RPC server implementations may choose to block and return the
     * capacity or return -1 indicating that you'll have to come back later for it.
     */
    @Optional
    long [] getCapacities();
    
    LogicalUsedCapacityInformation setCapacities( final long [] value );
}
