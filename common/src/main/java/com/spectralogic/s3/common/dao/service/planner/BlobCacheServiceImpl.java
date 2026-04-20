/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.planner;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.planner.BlobCache;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.platform.cache.CacheUtils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.*;

final class BlobCacheServiceImpl extends BaseService<BlobCache>
    implements BlobCacheService
{
    BlobCacheServiceImpl()
    {
        super( BlobCache.class );
    }

    public File getFile( final UUID blobId )
    {
        Validations.verifyNotNull( "Blob id", blobId );
        final BlobCache bc = retrieveByBlobId( blobId );
        if ( null == bc || bc.getState() == CacheEntryState.PENDING_DELETE)
        {
            return null;
        }

        update( bc.setLastAccessed(new Date(System.currentTimeMillis())), BlobCache.LAST_ACCESSED );
        return new File( bc.getPath() );
    }

    public File getFileIffInCache( final UUID blobId )
    {
        Validations.verifyNotNull( "Blob id", blobId );
        final BlobCache bc = retrieveByBlobId( blobId );
        if ( null == bc || CacheEntryState.IN_CACHE != bc.getState() )
        {
            return null;
        }

        update( bc.setLastAccessed(new Date(System.currentTimeMillis())), BlobCache.LAST_ACCESSED );
        return new File( bc.getPath() );
    }


    public boolean isInCache(final UUID blobId )
    {
        final BlobCache bc = retrieveByBlobId( blobId );
        return bc != null && bc.getState() != CacheEntryState.PENDING_DELETE;
    }

    public boolean anyDeletePending(Set<UUID> blobIds) {
        return any(Require.all(
                Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE),
                Require.beanPropertyEqualsOneOf(BlobCache.BLOB_ID, blobIds)
        ));
    }

    public boolean deletedBlobCachesExist() {
        return any(Require.all(
                Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)),
                Require.beanPropertyNull(BlobCache.BLOB_ID)));
    }


    //All blob caches whose blobs have been deleted and are not already pending delete.
    public EnhancedIterable<BlobCache> getDeletedBlobCaches() {
        return retrieveAll(Require.all(
                Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)),
                Require.beanPropertyNull(BlobCache.BLOB_ID)
        )).toIterable();
    }

    public long getBlobCachesCount() {
        return getCount(Require.not(Require.beanPropertyEquals(BlobCache.STATE, CacheEntryState.PENDING_DELETE)));
    }

    public boolean contains( final UUID blobId )
    {
        Validations.verifyNotNull( "Id", blobId );
        final BlobCache bc = retrieveByBlobId( blobId );
        if ( null == bc )
        {
            return false;
        }
        return ( CacheEntryState.IN_CACHE == bc.getState() );
    }

    @Override
    public BlobCache allocate(final UUID blobId, final long size, final CacheFilesystem filesystem) {
        final BlobCache bc = BeanFactory.newBean( BlobCache.class )
                .setSizeInBytes( size )
                .setState( CacheEntryState.ALLOCATED )
                .setLastAccessed( new Date(System.currentTimeMillis()) )
                .setPath(CacheUtils.getPath(filesystem, blobId))
                .setBlobId( blobId );
        create(bc);
        return bc;
    }

    @Override
    public long cacheEntryLoaded( final BlobCache bc, final boolean cacheSafetyEnabled)
    {
        final UUID blobId = bc.getBlobId();
        final Blob blob = getServiceManager().getRetriever( Blob.class ).attain( blobId );
        final long blobLength = blob.getLength();
        if ( bc.getState() == CacheEntryState.PENDING_DELETE )
        {
            throw new IllegalStateException(
                    "Cannot load cache entry " + blobId + " because it is pending delete." );
        }
        if ( bc.getState() == CacheEntryState.IN_CACHE )
        {
            throw new IllegalStateException(
                    "Cannot load cache entry " + blobId + " because it is already loaded." );
        }
        final Path file = Paths.get( bc.getPath() );
        final long actualSize;
        try
        {
            actualSize = Files.size( file );
        }
        catch ( IOException ex )
        {
            throw new IllegalStateException( ex );
        }

        try {
            final long cacheEntrySize = bc.getSizeInBytes();
            if (actualSize < cacheEntrySize) {
                if (actualSize != blobLength) {
                    Files.delete(file);
                    throw new FailureTypeObservableException(GenericFailure.CONFLICT,
                            "Expected blob size for blob " + bc.getBlobId() + " was " + blobLength
                                    + "B, but file was " + actualSize + "B.");
                }
            } else if (actualSize > cacheEntrySize) {
                Files.delete(file);
                throw new FailureTypeObservableException(GenericFailure.CONFLICT,
                        "Cache entry " + blobId + " was stated as being " + cacheEntrySize
                                + "B, but was " + actualSize + "B.");
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        final String validName = file.toAbsolutePath() + CacheUtils.CACHE_FILE_VALID_SUFFIX;
        final Path path = Paths.get( validName );
        if ( !Files.exists( path ) )
        {
            try
            {
                final Set<OpenOption> options = new HashSet<>();
                options.add( WRITE );
                if ( cacheSafetyEnabled )
                {
                    options.add( SYNC );
                }
                options.add( CREATE );

                final FileChannel channel = FileChannel.open( path, options);
                // Not sure if we need this or not since no data is written...
                channel.force( false );
                channel.close();
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        //NOTE: This will almost always be 0 except in Vail workflows
        final long freedSpace = bc.getSizeInBytes() - blobLength;
        update( bc.setState( CacheEntryState.IN_CACHE ).setSizeInBytes(blobLength),
                BlobCache.STATE, BlobCache.SIZE_IN_BYTES );
        return freedSpace;
    }

}
