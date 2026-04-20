/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.domain.planner;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.*;

import java.util.Date;
import java.util.UUID;

@Indexes(
{
    @Index( BlobCache.LAST_ACCESSED )
})
@UniqueIndexes(
{
    @Unique({ BlobCache.BLOB_ID })
})
public interface BlobCache extends DatabasePersistable
{
    String BLOB_ID = "blobId";

    @Optional
    @CascadeDelete( CascadeDelete.WhenReferenceIsDeleted.SET_NULL )
    @References( Blob.class )
    UUID getBlobId();

    BlobCache setBlobId( final UUID value );

    String LAST_ACCESSED = "lastAccessed";

    @SortBy
    Date getLastAccessed();

    BlobCache setLastAccessed( final Date lastAccessed );


    String SIZE_IN_BYTES = "sizeInBytes";

    long getSizeInBytes();
    
    BlobCache setSizeInBytes( final long value );
    

    String STATE = "state";

    CacheEntryState getState();
    
    BlobCache setState( final CacheEntryState value );


    String PATH = "path";

    String getPath();

    BlobCache setPath( final String value );
}
