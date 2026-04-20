/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;

public interface S3DataReplicationRule
    extends DatabasePersistable, PublicCloudDataReplicationRule< S3DataReplicationRule >
{
    @References( S3Target.class )
    UUID getTargetId();
    
    
    String INITIAL_DATA_PLACEMENT = "initialDataPlacement";
    
    @DefaultEnumValue( "STANDARD_IA" )
    S3InitialDataPlacementPolicy getInitialDataPlacement();
    
    S3DataReplicationRule setInitialDataPlacement( final S3InitialDataPlacementPolicy value );
}
