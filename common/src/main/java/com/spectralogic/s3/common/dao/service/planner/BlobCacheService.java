/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.planner;

import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

import java.io.File;
import java.util.Set;
import java.util.UUID;

public interface BlobCacheService
    extends BeansRetriever<BlobCache>, BeanUpdater< BlobCache >, BeanDeleter, BeanCreator< BlobCache >
{
    default BlobCache retrieveByBlobId(final UUID blobId) {
        return retrieve( Require.beanPropertyEquals(BlobCache.BLOB_ID, blobId) );
    }

    File getFile(final UUID blobId );

    File getFileIffInCache( final UUID blobId );

    boolean isInCache(final UUID blobId );

    boolean anyDeletePending(Set<UUID> blobIds);

    EnhancedIterable<BlobCache> getDeletedBlobCaches();

    boolean deletedBlobCachesExist();

    long getBlobCachesCount();

    boolean contains( final UUID blobId );

    BlobCache allocate(UUID blobId, long size, CacheFilesystem filesystem);

    long cacheEntryLoaded(BlobCache bc, boolean cacheSafetyEnabled);
}
