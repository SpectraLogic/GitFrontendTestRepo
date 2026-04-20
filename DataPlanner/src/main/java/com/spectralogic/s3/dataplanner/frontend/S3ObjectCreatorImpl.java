/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.common.rpc.dataplanner.domain.S3ObjectToCreate;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.s3.dataplanner.frontend.api.S3ObjectCreator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.render.BytesRenderer;
import org.apache.log4j.Logger;

import java.text.Normalizer;
import java.util.*;

final class S3ObjectCreatorImpl implements S3ObjectCreator
{
    S3ObjectCreatorImpl( 
            final long preferredBlobSizeInBytes,
            final long maxBlobSizeInBytes,
            final BlobbingPolicy blobbingPolicy,
            final S3ObjectService ignoreNamingConflictsObjectService,
            final UUID bucketId,
            final Set< S3ObjectToCreate > objectsToCreate )
    {
        Validations.verifyNotNull( "Bucket", bucketId );
        Validations.verifyNotNull( "Objects to create", objectsToCreate );
        Validations.verifyNotNull( "Blobbing policy", blobbingPolicy );
        
        m_preferredBlobSizeInBytes = Math.min( preferredBlobSizeInBytes, maxBlobSizeInBytes );
        m_maxBlobSizeInBytes = maxBlobSizeInBytes;
        m_bucketId = bucketId;
        m_blobbingPolicy = blobbingPolicy;
        initialize( objectsToCreate );
        if ( null != ignoreNamingConflictsObjectService )
        {
            ignoreNamingConflicts( ignoreNamingConflictsObjectService );
        }
    }


    private void initialize( final Set< S3ObjectToCreate > objectsToCreate )
    {
        long totalSize = 0;
        for ( final S3ObjectToCreate otc : objectsToCreate )
        {
            final String name = otc.getName();
            final S3Object o = BeanFactory.newBean( S3Object.class )
                    .setBucketId( m_bucketId )
                    .setName( name )
                    .setType( S3ObjectType.fromObjectName( name ) );
            o.setId( UUID.randomUUID() );
            if ( S3ObjectType.FOLDER == S3ObjectType.fromObjectName( o.getName() )
                    && 0 != otc.getSizeInBytes() )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Folders cannot have data associated with them.  " + name
                        + " is a folder." );
            }

            final long blobSize = getBlobSize( otc.getSizeInBytes() );
            final int numBlobs = getNumberOfBlobs( otc.getSizeInBytes(), blobSize );
            for ( int i = 0; i < Math.max( 1, numBlobs ); ++i )
            {
                m_blobs.add( BeanFactory.newBean( Blob.class )
                        .setObjectId( o.getId() )
                        .setByteOffset( i * blobSize )
                        .setLength( Math.max(
                                0,
                                Math.min(
                                        blobSize,
                                        otc.getSizeInBytes() - i * blobSize ) ) ) );
            }
            m_objects.add( o );
            m_objectSizes.put( o.getName(), Long.valueOf( otc.getSizeInBytes() ) );
            totalSize += otc.getSizeInBytes();
        }
        m_totalSize = totalSize;

        for ( final Blob blob : m_blobs )
        {
            blob.setId( UUID.randomUUID() );
        }
    }


    private long getBlobSize( final long objectSizeInBytes )
    {
        switch ( m_blobbingPolicy )
        {
            case ENABLED:
                return m_preferredBlobSizeInBytes;
            case DISABLED:
                if ( objectSizeInBytes > m_maxBlobSizeInBytes )
                {
                    throw new DataPlannerException( 
                            GenericFailure.CONFLICT,
                            "Multiple blobs are required for objects exceeding " 
                            + new BytesRenderer().render( m_maxBlobSizeInBytes ) 
                            + ", but blobbing isn't allowed." );
                }
                return m_maxBlobSizeInBytes;
            default:
                throw new UnsupportedOperationException( "No code for: " + m_blobbingPolicy );
        }
    }
    
    
    private int getNumberOfBlobs( final long objectSizeInBytes, final long blobSize )
    {
        if ( 0 == objectSizeInBytes )
        {
            return 0;
        }
        if ( 0 > objectSizeInBytes )
        {
            return 1; // this is a multi-part upload, which will have a single, zero-length blob initially
        }
        
        final int fullBlobs = (int)( objectSizeInBytes / blobSize );
        return ( 0 == objectSizeInBytes % blobSize ) ? fullBlobs : fullBlobs + 1;
    }
    
    
    private void ignoreNamingConflicts( final S3ObjectService objectService )
    {
        final Set< S3Object > existingObjects = objectService.retrieveAll( Require.all( 
                Require.beanPropertyEquals( S3Object.BUCKET_ID, m_bucketId ),
                Require.beanPropertyEqualsOneOf(
                        S3Object.NAME, 
                        BeanUtils.extractPropertyValues( m_objects, S3Object.NAME ) ) ) ).toSet();
        for ( final S3Object existingObject : existingObjects )
        {
            final long existingLength = 
                    objectService.getSizeInBytes( CollectionFactory.toSet( existingObject.getId() ) );
            final long newLength = m_objectSizes.get( existingObject.getName() ).longValue();
            if ( newLength != existingLength )
            {
                throw new DataPlannerException( 
                        GenericFailure.CONFLICT,
                        "Object '" + existingObject.getName() 
                        + "' already exists.  Cannot ignore since the object being uploaded is of length "
                        + newLength + ", but the existing object is of length " + existingLength + "." );
            }
        }
        
        final Set< UUID > whackedObjectIds = new HashSet<>();
        final Set< String > existingNames = 
                BeanUtils.extractPropertyValues( existingObjects, S3Object.NAME );
        for ( final S3Object o : new HashSet<>( m_objects ) )
        {
            if ( existingNames.contains( o.getName() ) )
            {
                whackedObjectIds.add( o.getId() );
                m_objects.remove( o );
            }
        }
        for ( final Blob b : new HashSet<>( m_blobs ) )
        {
            if ( whackedObjectIds.contains( b.getObjectId() ) )
            {
                m_blobs.remove( b );
            }
        }
        
        LOG.info( "Removed " + whackedObjectIds.size() + " existing objects from job to create." );
    }
    
    
    public Set< S3Object > getObjects()
    {
        return new HashSet<>( m_objects );
    }
    
    
    public Set< Blob > getBlobs()
    {
        return new HashSet<>( m_blobs );
    }

    @Override
    public long getTotalSize() {
        return m_totalSize;
    }


    public void commit( final S3ObjectService objectService, final BlobService blobService )
    {
        objectService.create( m_objects );
        LOG.info( "Created " + m_objects.size() + " objects in bucket " + m_bucketId + "." );
        
        blobService.create( m_blobs );
        LOG.info( "Created " + m_blobs.size() + " blobs in bucket " + m_bucketId + "." );
    }


    private long m_totalSize;
    private final long m_preferredBlobSizeInBytes;
    private final long m_maxBlobSizeInBytes;
    private final UUID m_bucketId;
    private final Set< S3Object > m_objects = new HashSet<>();
    private final Map< String, Long > m_objectSizes = new HashMap<>();
    private final Set< Blob > m_blobs = new HashSet<>();
    private final BlobbingPolicy m_blobbingPolicy;
    
    private final static Logger LOG = Logger.getLogger( S3ObjectCreatorImpl.class );
}
