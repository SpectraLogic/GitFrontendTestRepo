/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.References;

public interface ImportPersistenceTargetDirective< T > extends ImportDirective< T >
{
    String STORAGE_DOMAIN_ID = "storageDomainId";

    /**
     * @return the storage domain to assign the persistence target to upon import (will be automatically
     * determined if possible if not specified)
     */
    @Optional
    @References( StorageDomain.class )
    UUID getStorageDomainId();
    
    T setStorageDomainId( final UUID value );
    
    
    String VERIFY_DATA_PRIOR_TO_IMPORT = "verifyDataPriorToImport";
    
    @DefaultBooleanValue( true )
    boolean isVerifyDataPriorToImport();
    
    T setVerifyDataPriorToImport( final boolean value );
    
    
    String VERIFY_DATA_AFTER_IMPORT = "verifyDataAfterImport";
    
    @Optional
    BlobStoreTaskPriority getVerifyDataAfterImport();
    
    T setVerifyDataAfterImport( final BlobStoreTaskPriority value );
}
