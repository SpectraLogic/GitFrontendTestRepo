/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( @Index( StorageDomainFailure.DATE ) )
public interface StorageDomainFailure
    extends DatabasePersistable, Failure< StorageDomainFailure, StorageDomainFailureType >
{
    String STORAGE_DOMAIN_ID = "storageDomainId";
    
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( StorageDomain.class )
    UUID getStorageDomainId();
    
    StorageDomainFailure setStorageDomainId( final UUID value );
    
    
    StorageDomainFailureType getType();
    
    StorageDomainFailure setType( final StorageDomainFailureType value );
}
