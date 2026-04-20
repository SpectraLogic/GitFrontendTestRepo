/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface Ds3TargetDataPolicies extends SimpleBeanSafeToProxy
{
    String DATA_POLICIES = "dataPolicies";
    
    DataPolicy [] getDataPolicies();
    
    void setDataPolicies( final DataPolicy [] value );
}
