/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A rule part of a {@link DataPolicy} to replicate data to the specified replication target.
 */
@UniqueIndexes(
{
    @Unique( { DataPlacement.DATA_POLICY_ID, DataReplicationRule.TARGET_ID } )
})
public interface Ds3DataReplicationRule
    extends DatabasePersistable, DataReplicationRule< Ds3DataReplicationRule >
{
    @References( Ds3Target.class )
    UUID getTargetId();
    
    
    String TARGET_DATA_POLICY = "targetDataPolicy";
    
    @Optional
    String getTargetDataPolicy();
    
    Ds3DataReplicationRule setTargetDataPolicy( final String value );
}
