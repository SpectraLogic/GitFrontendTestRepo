/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * Any pool in state {@link PoolState#IMPORT_IN_PROGRESS} will have one of these records to keep track of the
 * parameters of the import directive.  This record will be used to restart the import with the same 
 * parameters as was originally requested in the import.
 */
@UniqueIndexes( @Unique( ImportPoolDirective.POOL_ID ) )
public interface ImportPoolDirective 
    extends DatabasePersistable, ImportPersistenceTargetDirective< ImportPoolDirective >
{
    String POOL_ID = "poolId";
    
    @CascadeDelete
    @References( Pool.class )
    UUID getPoolId();
    
    ImportPoolDirective setPoolId( final UUID value );
}
