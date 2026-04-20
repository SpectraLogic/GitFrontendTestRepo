/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.lang.References;

public interface DataPlacement< T > extends SimpleBeanSafeToProxy, Identifiable
{
    String STATE = "state";
    
    DataPlacementRuleState getState();
    
    T setState( final DataPlacementRuleState value );
    
    
    String DATA_POLICY_ID = "dataPolicyId";
    
    @References( DataPolicy.class )
    UUID getDataPolicyId();
    
    T setDataPolicyId( final UUID value );
}
