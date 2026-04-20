/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * A rule part of a {@link DataPolicy} to persist data at the specified {@link StorageDomain}.
 */
@UniqueIndexes(
{
    @Unique( { DataPlacement.DATA_POLICY_ID, DataPersistenceRule.STORAGE_DOMAIN_ID } )
})
public interface DataPersistenceRule extends DatabasePersistable, DataPlacement< DataPersistenceRule >
{
    String TYPE = "type";
    
    DataPersistenceRuleType getType();
    
    DataPersistenceRule setType( final DataPersistenceRuleType value );
    
    
    String ISOLATION_LEVEL = "isolationLevel";
    
    DataIsolationLevel getIsolationLevel();
    
    DataPersistenceRule setIsolationLevel( final DataIsolationLevel value );
    
    
    String STORAGE_DOMAIN_ID = "storageDomainId";
    
    @References( StorageDomain.class )
    UUID getStorageDomainId();
    
    DataPersistenceRule setStorageDomainId( final UUID value );
    
    
    String MINIMUM_DAYS_TO_RETAIN = "minimumDaysToRetain";
    
    /**
     * @return the minimum number of days a {@link Blob} is to be retained on the target 
     * {@link #STORAGE_DOMAIN_ID} once it is placed there (the {@link Blob} may be retained longer but no 
     * less than this minimum number of days)
     */
    @Optional
    Integer getMinimumDaysToRetain();
    
    DataPersistenceRule setMinimumDaysToRetain( final Integer value );
}
