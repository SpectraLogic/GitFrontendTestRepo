/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;

public interface DataReplicationRule< T extends DataReplicationRule< T > > extends DataPlacement< T >
{
    String TYPE = "type";
    
    DataReplicationRuleType getType();
    
    T setType( final DataReplicationRuleType value );
    
    
    String TARGET_ID = "targetId";
    
    UUID getTargetId();
    
    T setTargetId( final UUID value );
    

    String REPLICATE_DELETES = "replicateDeletes";
    
    @DefaultBooleanValue( true )
    boolean isReplicateDeletes();
    
    T setReplicateDeletes( final boolean value );
}
