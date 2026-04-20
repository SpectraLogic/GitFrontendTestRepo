/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.shutdown.BaseShutdownable;

final class PoolApplicationContentsReader extends BaseShutdownable
{
    PoolApplicationContentsReader( final Pool pool )
    {
        m_pool = pool;
        Validations.verifyNotNull( "Pool", m_pool );
    }
    
    
    synchronized S3ObjectsOnMedia getNextChunk( final int maxBlobsInChunk )
    {
        m_foundBlob = ( null == m_lastBlobId );
        m_remainingBlobs = maxBlobsInChunk;
        
        final List< BucketOnMedia > buckets = new ArrayList<>();
        final Path root = Paths.get( m_pool.getMountpoint() );
        for ( final Path bucketDir : PoolUtils.getDirectories( root ) )
        {
            if ( !getBucketNameFromBucketDirectory( bucketDir ).equals( PoolUtils.TRASH_DIRECTORY ) )
            {
                buckets.add( processBucket( bucketDir ) );
            }
            buckets.remove( null );
        }
        if ( buckets.isEmpty() )
        {
            return null;
        }
        
        final S3ObjectsOnMedia retval = BeanFactory.newBean( S3ObjectsOnMedia.class );
        retval.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, buckets ) );
        return retval;
    }
    
    
    private BucketOnMedia processBucket( final Path bucketDir )
    {
        if ( 0 == m_remainingBlobs )
        {
            return null;
        }
        
        final BucketOnMedia bucket = BeanFactory.newBean( BucketOnMedia.class );
        final List< S3ObjectOnMedia > objects = new ArrayList<>();
        PoolUtils.getDirectories( bucketDir )
                 .stream()
                 .flatMap( objectHashDir -> PoolUtils.getDirectories( objectHashDir )
                                                     .stream() )
                 .forEach( objectDir -> {
                     objects.add( processObject( objectDir ) );
                     objects.remove( null );
                 } );
        
        if ( !m_foundBlob )
        {
            return null;
        }
        
        bucket.setBucketName( getBucketNameFromBucketDirectory(bucketDir) );
        bucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, objects ) );
        
        return bucket;
    }


    private String getBucketNameFromBucketDirectory( final Path bucketDir ) {
        return bucketDir.getFileName().toString();
    }
    
    
    private S3ObjectOnMedia processObject( final Path objectDir )
    {
        if ( 0 == m_remainingBlobs )
        {
            return null;
        }
        
        PoolUtils.verifyObjectDir( objectDir );
        final S3ObjectOnMedia object = BeanFactory.newBean( S3ObjectOnMedia.class );
        final List< BlobOnMedia > blobs = new ArrayList<>();
        for ( final Path blobFile : PoolUtils.getEntries( objectDir ) )
        {
            try
            {
                UUID.fromString( blobFile.getFileName().toString() );
                blobs.add( processBlob( blobFile ) );
                blobs.remove( null );
            }
            catch ( final RuntimeException ex )
            {
                Validations.verifyNotNull( "It's not a blob file.", ex );
            }
        }
        
        if ( !m_foundBlob )
        {
            return null;
        }
        
        final Properties props = getProperties( PoolUtils.getPropsFile( objectDir ) );
        final Properties metadataProps = getProperties( PoolUtils.getMetadataFile( objectDir ) );
        object.setId( UUID.fromString( objectDir.getFileName().toString() ) );
        object.setObjectName( props.getProperty( NameObservable.NAME ) );
        object.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, blobs ) );
        
        final List< S3ObjectMetadataKeyValue > metadatas = new ArrayList<>();
        for (final Map.Entry<Object, Object> metadataPropEntry : metadataProps.entrySet() )
        {
            final S3ObjectMetadataKeyValue metadata = BeanFactory.newBean( S3ObjectMetadataKeyValue.class );
            metadata.setKey( metadataPropEntry.getKey().toString() );
            metadata.setValue( metadataPropEntry.getValue().toString() );
            metadatas.add( metadata );
        }
        object.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
        
        return object;
    }
    
    
    private BlobOnMedia processBlob( final Path blobFile )
    {
        if ( 0 == m_remainingBlobs )
        {
            return null;
        }
        
        final BlobOnMedia blob = BeanFactory.newBean( BlobOnMedia.class );
        blob.setId( UUID.fromString( blobFile.getFileName().toString() ) );
        if ( m_foundBlob )
        {
            m_lastBlobId = blob.getId();
        }
        else
        {
            if ( !blob.getId().equals( m_lastBlobId ) )
            {
                return null;
            }
            m_foundBlob = true;
            m_lastBlobId = UUID.randomUUID();
        }
        
        --m_remainingBlobs;
        final Properties props = getProperties( PoolUtils.getPropsFile( blobFile ) );
        blob.setChecksum( props.getProperty( ChecksumObservable.CHECKSUM ) );
        blob.setChecksumType( ChecksumType.valueOf( props.getProperty( ChecksumObservable.CHECKSUM_TYPE ) ) );
        try
        {
            blob.setLength( Files.size( blobFile ) );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to get blob file size " + blobFile + ".", ex );        }
        blob.setOffset( Long.parseLong( props.getProperty( Blob.BYTE_OFFSET ) ) );
        
        return blob;
    }
    
    
    private Properties getProperties( final Path propsFile )
    {
        try
        {
            final Properties retval = new Properties();
            final Reader reader = Files.newBufferedReader( propsFile );
            retval.load( reader );
            reader.close();
            return retval;
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to load properties at " + propsFile + ".", ex );
        }
    }
    
    
    private boolean m_foundBlob;
    private int m_remainingBlobs;
    private UUID m_lastBlobId;
    
    private final Pool m_pool;
}
