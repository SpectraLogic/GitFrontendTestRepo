/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.*;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;

@UniqueIndexes(
{
    @Unique({ LocalBlobDestination.ENTRY_ID, LocalBlobDestination.STORAGE_DOMAIN_ID })
})
public interface LocalBlobDestination extends DatabasePersistable
{
    String ENTRY_ID = "entryId";

    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( JobEntry.class )
    UUID getEntryId();
    
    LocalBlobDestination setEntryId(final UUID value );
    
    
    String STORAGE_DOMAIN_ID = "storageDomainId";
    
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( StorageDomain.class )
    UUID getStorageDomainId();
    
    LocalBlobDestination setStorageDomainId(final UUID value );


    String ISOLATED_BUCKET_ID = "isolatedBucketId";


    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Bucket.class )
    @Optional
    UUID getIsolatedBucketId();

    LocalBlobDestination setIsolatedBucketId(final UUID value );


    String PERSISTENCE_RULE_ID = "persistenceRuleId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    @References( DataPersistenceRule.class )
    UUID getPersistenceRuleId();

    LocalBlobDestination setPersistenceRuleId(final UUID value );


    String BLOB_STORE_STATE = "blobStoreState";

    @DefaultEnumValue( "PENDING" )
    JobChunkBlobStoreState getBlobStoreState();

    LocalBlobDestination setBlobStoreState(final JobChunkBlobStoreState value );
}
