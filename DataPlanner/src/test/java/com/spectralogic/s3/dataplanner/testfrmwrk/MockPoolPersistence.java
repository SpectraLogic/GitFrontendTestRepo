/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.testfrmwrk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class MockPoolPersistence extends BaseShutdownable
{
    public MockPoolPersistence( final DatabaseSupport dbSupport, final UUID poolPartitionId )
    {
        try
        {
            m_root = Files.createTempDirectory( MockPoolPersistence.class.getSimpleName() ).toFile();
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        
        m_serviceManager = dbSupport.getServiceManager();
        m_pool = new MockDaoDriver( dbSupport ).createPool(
                PoolType.NEARLINE, m_root.getAbsolutePath(), poolPartitionId, null, true );
        
        addShutdownListener( new CleanupFilesystemShutdownListener() );
//        Files.deleteIfExists( PoolUtils.getCompactedUnknownMarkPath( getPool() ) );
    }
    
    
    private final class CleanupFilesystemShutdownListener extends CriticalShutdownListener
    {
        @Override
        public void shutdownOccurred()
        {
            try
            {
                Files.walk( Paths.get( m_root.toString() ) )
                     .sorted( Comparator.reverseOrder() )
                     .map( Path::toFile )
                     .forEach( File::delete );
            }
            catch ( final IOException ex )
            {
                throw new RuntimeException( ex );
            }
        }
    } // end inner class def
    
    
    public Pool getPool()
    {
        return m_pool;
    }
    
    
    private void create( final Bucket bucket, final S3Object object, final Blob blob )
    {
        final Path file = PoolUtils.getPath( m_pool, bucket.getName(), object.getId(), blob.getId() );
        try
        {
            Files.createDirectories( file.getParent() );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to create directories: " + file.getParent(), ex );
        }
        try
        {
            final OutputStream out = Files.newOutputStream( file );
            final byte [] data = new byte[ (int)blob.getLength() ];
            for ( int i = 0; i < data.length; ++i )
            {
                data[ i ] = (byte)( i % 100 );
            }
            out.write( data );
            out.close();
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        setupBucketDir( bucket );
        writeBlobProps( blob, file );
        writeObjectProps( bucket, object );
        writeObjectMetadata( bucket, object );
    }
    
    
    public void create( final Blob... blobs )
    {
        for ( final Blob blob : blobs )
        {
            create( blob );
        }
    }
    
    
    public void create( final Bucket bucket, final S3Object object, final Blob... blobs )
    {
        for ( final Blob blob : blobs )
        {
            create( bucket, object, blob );
        }
    }
    
    
    private void create( final Blob blob )
    {
        final S3Object object = m_serviceManager.getRetriever( S3Object.class )
                                                .attain( blob.getObjectId() );
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class )
                                              .attain( object.getBucketId() );
        create( bucket, object, blob );
    }
    
    
    public void deleteBlob( final UUID blobId )
    {
        final Blob blob = m_serviceManager.getRetriever( Blob.class )
                                          .attain( blobId );
        final S3Object object = m_serviceManager.getRetriever( S3Object.class )
                                                .attain( blob.getObjectId() );
        final Bucket bucket = m_serviceManager.getRetriever( Bucket.class )
                                              .attain( object.getBucketId() );
        
        final Path file = PoolUtils.getPath( m_pool, bucket.getName(), object.getId(), blob.getId() );
        if ( !Files.exists( file ) )
        {
            throw new RuntimeException( "File does not exist: " + file );
        }
        try
        {
            Files.deleteIfExists( file );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to delete: " + file, ex );
        }
        
        final Path propsFile = PoolUtils.getPropsFile( file );
        if ( !Files.exists( propsFile ) )
        {
            throw new RuntimeException( "Props file does not exist: " + propsFile );
        }
        try
        {
            Files.deleteIfExists( propsFile );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to delete: " + propsFile, ex );
        }
    }
    
    
    public void create( final Bucket ... buckets )
    {
        for ( final Bucket b : buckets )
        {
            create( b );
        }
    }
    
    
    private void create( final Bucket bucket )
    {
        setupBucketDir( bucket );
    }
    
    
    public void create(
            final Bucket bucket, 
            final S3Object object,
            final S3ObjectMetadataKeyValue [] metadatas )
    {
        setupBucketDir( bucket );
        writeObjectProps( bucket, object );
        writeObjectMetadata(
                bucket,
                object, 
                metadatas );
    }
    
    
    private void setupBucketDir( final Bucket bucket )
    {
        final Path bucketDir = PoolUtils.getPath( m_pool, bucket.getName(), null, null );
        try
        {
            Files.createDirectories( bucketDir );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to create directories: " + bucketDir, ex );
        }
    }
    
    
    private void writeBlobProps( final Blob blob, final Path blobDir )
    {
        final Path fileBlobProps = PoolUtils.getPropsFile( blobDir );
        final Properties blobProps = new Properties();
        blobProps.setProperty( Blob.BYTE_OFFSET, String.valueOf( blob.getByteOffset() ) );
        blobProps.setProperty( ChecksumObservable.CHECKSUM_TYPE, blob.getChecksumType().toString() );
        blobProps.setProperty( ChecksumObservable.CHECKSUM, blob.getChecksum() );
        try
        {
            final OutputStream out = Files.newOutputStream( fileBlobProps );
            blobProps.store( out, "---No Comment---" );
            out.close();
        }
        catch ( final IOException ex1 )
        {
            throw new RuntimeException( "Failed to write blob properties file: " + fileBlobProps, ex1 );
        }
    }
    
    
    private void writeObjectMetadata(
            final Bucket bucket,
            final S3Object object,
            final S3ObjectMetadataKeyValue ... metadatas )
    {
        final Path objectDir = PoolUtils.getPath( m_pool, bucket.getName(), object.getId(), null );
        try
        {
            Files.createDirectories( objectDir );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to create directories: " + objectDir, ex );
        }
    
        final Path fileObjectMetadata = PoolUtils.getMetadataFile( objectDir );
        if ( Files.exists( fileObjectMetadata ) )
        {
            return;
        }
        
        final Set< S3ObjectProperty > objectMetadata = m_serviceManager
                .getRetriever( S3ObjectProperty.class )
                .retrieveAll(
                        Require.beanPropertyEquals( S3ObjectProperty.OBJECT_ID, object.getId() ) )
                .toSet();
        final Properties objectMetadataProps = new Properties();
        if ( null != object.getCreationDate() )
        {
        	objectMetadataProps.setProperty(
        			KeyValueObservable.CREATION_DATE,
        			String.valueOf( object.getCreationDate().getTime() ) ); 
        }
        for ( final S3ObjectMetadataKeyValue metadata : metadatas )
        {
            objectMetadataProps.setProperty( metadata.getKey(), metadata.getValue() );
        }
        for ( final S3ObjectProperty objProp : objectMetadata )
        {
            objectMetadataProps.setProperty( objProp.getKey(), objProp.getValue() );
        }
        try
        {
            final OutputStream out = Files.newOutputStream( fileObjectMetadata );
            objectMetadataProps.store(out, "---No Comment---");
            out.close();
        }
        catch ( final IOException ex1 )
        {
            throw new RuntimeException( "Failed to write metadata file: " + fileObjectMetadata, ex1 );
        }
    }
    
    
    private void writeObjectProps( final Bucket bucket, final S3Object object )
    {
        final Path objectDir = PoolUtils.getPath( m_pool, bucket.getName(), object.getId(), null );
        try
        {
            Files.createDirectories( objectDir );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to create directories: " + objectDir, ex );
        }
    
        final Path fileObjectProps = PoolUtils.getPropsFile( objectDir );
        if ( Files.exists( fileObjectProps ) )
        {
            return;
        }
        
        final Properties bucketNameProps = new Properties();
        bucketNameProps.setProperty( NameObservable.NAME, object.getName() );
        try
        {
            final OutputStream out = Files.newOutputStream( fileObjectProps );
            bucketNameProps.store( out, "---No Comment---" );
            out.close();
        }
        catch ( final IOException ex1 )
        {
            throw new RuntimeException( "Failed to write props file: " + fileObjectProps, ex1 );
        }
    }
    
    
    public void assertBlobsPersisted( final Blob ... blobs )
    {
        assertBlobsPersisted( CollectionFactory.toSet( blobs ) );
    }
    
    
    public void assertBlobsPersisted( final Set< Blob > blobs )
    {
        assertBlobsPersisted( null, blobs );
    }
    
    
    public void assertBlobsPersisted( 
            Set< UUID > objectIds, 
            final Set< Blob > blobs )
    {
        assertBlobsPersisted( objectIds, blobs, null );
    }
    
    
    public void assertBlobsPersisted( 
            Set< UUID > objectIds, 
            final Set< Blob > blobs,
            final Map< UUID, Integer > blobsPerObjectOverrides )
    {
        if ( null == objectIds )
        {
            objectIds = new HashSet<>();
        }
        
        final Map< String, Map< UUID, Set< UUID > > > expectedFiles = 
                getExpectedFiles( objectIds, blobs );
        final Map< String, Map< UUID, Set< UUID > > > actualFiles = getActualFiles();
        if ( !expectedFiles.equals( actualFiles ) )
        {
            throw new RuntimeException( "Expected " + expectedFiles + ", but was " + actualFiles + "." );
        }
        
        final Map< UUID, Integer > blobsPerObject = new HashMap<>();
        for ( final UUID objectId : objectIds )
        {
            blobsPerObject.put( objectId, Integer.valueOf( 0 ) );
        }
        for ( final Blob blob : blobs )
        {
            blobsPerObject.put( 
                    blob.getObjectId(), 
                    Integer.valueOf( blobsPerObject.get( blob.getObjectId() ).intValue() + 1 ) );
        }
        
        // file metadata
        final Map< UUID, Properties > expectedObjectsMetadata = new HashMap<>();
        for ( final UUID objectId : objectIds )
        {
            final Properties expectedObjectMetadata = new Properties();
            final S3Object object = m_serviceManager.getRetriever( S3Object.class ).retrieve( objectId );
            if ( null != object.getCreationDate() )
            {
                expectedObjectMetadata.put( 
                        KeyValueObservable.CREATION_DATE, 
                        String.valueOf( object.getCreationDate().getTime() ) );
            }
            if ( null != blobsPerObjectOverrides )
            {
                expectedObjectMetadata.put( 
                        KeyValueObservable.TOTAL_BLOB_COUNT, 
                        String.valueOf( blobsPerObjectOverrides.containsKey( objectId ) ? 
                                blobsPerObjectOverrides.get( objectId )
                                : blobsPerObject.get( objectId ) ) );
            }
            final Set< S3ObjectProperty > objectMetadata = m_serviceManager
                    .getRetriever( S3ObjectProperty.class )
                    .retrieveAll(
                            Require.beanPropertyEquals( S3ObjectProperty.OBJECT_ID, object.getId() ) )
                    .toSet();
            for ( S3ObjectProperty objProp : objectMetadata )
            {
                expectedObjectMetadata.setProperty( objProp.getKey(), objProp.getValue() );
            }
            expectedObjectsMetadata.put( object.getId(), expectedObjectMetadata );
        }
        final Map< UUID, Properties > actualObjectsMetadata = getActualObjectMetadata();
        if ( null == blobsPerObjectOverrides )
        {
            for ( final Properties properties : actualObjectsMetadata.values() )
            {
                properties.remove( KeyValueObservable.TOTAL_BLOB_COUNT );
            }
        }
        if ( !expectedObjectsMetadata.equals( actualObjectsMetadata ) )
        {
            throw new RuntimeException( "Expected " + expectedObjectsMetadata
                    + ", but was " + actualObjectsMetadata + "." );
        }
        
        // file props
        final Map < UUID,  Properties  > expectedObjectsProperties = new HashMap <>();
        for ( UUID objectId : objectIds )
        {
            final Properties expectedObjectNameProps = new Properties();
            final S3Object object = m_serviceManager.getRetriever( S3Object.class ).retrieve( objectId );
            expectedObjectNameProps.setProperty( NameObservable.NAME, object.getName() );
            expectedObjectsProperties.put( object.getId(), expectedObjectNameProps );
        }
        final Map <UUID, Properties > actualObjectsProperties = getActualObjectProps();
        if ( !expectedObjectsProperties.equals( actualObjectsProperties ) )
        {
            throw new RuntimeException( "Expected " + expectedObjectsProperties
                    + ", but was " + actualObjectsProperties + "." );
        }

        // blob props
        final Map < UUID,  Properties  > expectedBlobProperties = new HashMap <>();
        for ( Blob blob : blobs )
        {
            final Properties expectedBlobProps = new Properties();
            expectedBlobProps.setProperty(Blob.BYTE_OFFSET, String.valueOf( blob.getByteOffset() ) );
            expectedBlobProps.setProperty(ChecksumObservable.CHECKSUM, blob.getChecksum() );
            expectedBlobProps.setProperty(ChecksumObservable.CHECKSUM_TYPE,
                    blob.getChecksumType().toString() );
            expectedBlobProperties.put( blob.getId(), expectedBlobProps );
        }
        final Map <UUID, Properties > actualBlobProperties = getActualBlobProps();
        if ( !expectedBlobProperties.equals( actualBlobProperties ) )
        {
            throw new RuntimeException( "Expected " + expectedBlobProperties
                    + ", but was " + actualBlobProperties + "." );
        }
    }
    
    
    public void assertBlobsNotPersisted( final Blob... blobs )
    {
        final Set< UUID > blobIds = CollectionFactory.toSet( blobs )
                                                     .stream()
                                                     .map( x -> x.getId() )
                                                     .collect( Collectors.toSet() );
        final Map< String, Map< UUID, Set< UUID > > > actualFiles = getActualFiles();
        actualFiles.values()
                   .forEach( setMap -> setMap.values()
                                             .forEach( set -> set.stream()
                                                                 .filter( blobIds::contains )
                                                                 .forEach( blob -> {
                                                                     throw new RuntimeException(
                                                                             "Found unexpected blob " + blob + " in " +
                                                                                     actualFiles );
                                                                 } ) ) );
    }
    
    
    private Map< String, Map< UUID, Set< UUID > > > getExpectedFiles( 
            final Set< UUID > objectIds, 
            final Set< Blob > blobs )
    {
        objectIds.addAll( BeanUtils.extractPropertyValues( blobs, Blob.OBJECT_ID ) );
        final Map< UUID, S3Object > objects = BeanUtils.toMap( 
                m_serviceManager.getRetriever( S3Object.class ).retrieveAll( objectIds ).toSet() );
        final Map< UUID, Bucket > buckets = BeanUtils.toMap( 
                m_serviceManager.getRetriever( Bucket.class ).retrieveAll( 
                BeanUtils.extractPropertyValues( objects.values(), S3Object.BUCKET_ID ) ).toSet() );
        
        final Map< String, Map< UUID, Set< UUID > > > retval = new HashMap<>();
        for ( final Bucket bucket : buckets.values() )
        {
            retval.put( bucket.getName(), new HashMap< UUID, Set< UUID > >() );
        }
        for ( final S3Object object : objects.values() )
        {
            retval.get( buckets.get( object.getBucketId() ).getName() ).put(
                    object.getId(), new HashSet< UUID >() );
        }
        for ( final Blob blob : blobs )
        {
            final S3Object o = objects.get( blob.getObjectId() );
            retval.get( buckets.get( o.getBucketId() ).getName() ).get( o.getId() ).add( blob.getId() );
        }
        
        return retval;
    }
    

    private Map< UUID, Properties > getActualObjectMetadata()
    {
        final Map< UUID, Properties > retval = new HashMap<>();
        for ( final File rootMember : toNonNull( m_root.listFiles() ) )
        {
            if ( rootMember.isDirectory() )
            {
                for ( final File hashDir : toNonNull( rootMember.listFiles() ) )
                {
                    for ( final File objectFile : toNonNull( hashDir.listFiles() ) )
                    {
                        if ( !objectFile.isDirectory()
                                && objectFile.getName().matches( ".*\\.metadata$" ) )
                        {
                            FileInputStream in;
                            try
                            {
                                in = new FileInputStream( objectFile );
                            }
                            catch ( FileNotFoundException ex1 )
                            {
                                throw new RuntimeException(
                                        "File not found reading expected bucket props file "
                                                + objectFile, ex1 );
                            }
                            final Properties objectMetadata = new Properties();
                            try
                            {
                                objectMetadata.load( in );
                                retval.put(
                                        UUID.fromString( objectFile.getName().replaceAll(
                                                ".metadata$", "" ) ), objectMetadata );
                                in.close();
                            }
                            catch ( IOException ex )
                            {
                                throw new RuntimeException(
                                        "IO exception reading expected bucket props file "
                                                + objectFile, ex );
                            }
                        }
                    }
                }
            }
        }
        return retval;
    }
    
    private Map< UUID, Properties > getActualObjectProps()
    {
        final Map< UUID, Properties > retval = new HashMap<>();
        for ( final File rootMember : toNonNull( m_root.listFiles() ) )
        {
            if ( rootMember.isDirectory() )
            {
                for ( final File hashDir : toNonNull( rootMember.listFiles() ) )
                {
                    for ( final File objectFile : toNonNull( hashDir.listFiles() ) )
                    {
                        if ( !objectFile.isDirectory()
                                && objectFile.getName().matches( ".*\\.props$" ) )
                        {
                            FileInputStream in;
                            try
                            {
                                in = new FileInputStream( objectFile );
                            }
                            catch ( FileNotFoundException ex1 )
                            {
                                throw new RuntimeException(
                                        "File not found reading expected bucket props file "
                                                + objectFile, ex1 );
                            }
                            final Properties objectProps= new Properties();
                            try
                            {
                                objectProps.load( in );
                                retval.put(
                                        UUID.fromString( objectFile.getName().replaceAll(
                                                ".props$", "" ) ), objectProps );
                                in.close();
                            }
                            catch ( IOException ex )
                            {
                                throw new RuntimeException(
                                        "IO exception reading expected bucket props file "
                                                + objectFile, ex );
                            }
                        }
                    }
                }
            }
        }
        return retval;
    }
    
    private Map< UUID, Properties > getActualBlobProps()
    {
        final Map< UUID, Properties > retval = new HashMap<>();
        for ( final File rootMember : toNonNull( m_root.listFiles() ) )
        {
            if ( rootMember.isDirectory() )
            {
                for ( final File hashDir : toNonNull( rootMember.listFiles() ) )
                {
                    for ( final File objectMember : toNonNull( hashDir.listFiles() ) )
                    {
                        if ( objectMember.isDirectory() )
                        {
                            for ( final File blobPropFile : toNonNull( objectMember.listFiles() ) )
                            {
                                if ( blobPropFile.getName().matches( ".*\\.props$" ) )
                                {
                                    FileInputStream in;
                                    try
                                    {
                                        in = new FileInputStream( blobPropFile );
                                    }
                                    catch ( FileNotFoundException ex1 )
                                    {
                                        throw new RuntimeException(
                                                "File not found reading expected bucket props file "
                                                        + blobPropFile, ex1 );
                                    }
                                    final Properties blobProps = new Properties();
                                    try
                                    {
                                        blobProps.load( in );
                                        retval.put(
                                                UUID.fromString( blobPropFile.getName().replaceAll(
                                                        ".props$", "" ) ), blobProps );
                                        in.close();
                                    }
                                    catch ( IOException ex )
                                    {
                                        throw new RuntimeException(
                                                "IO exception reading expected bucket props file "
                                                        + blobPropFile, ex );
                                    }
                                }
                            }
                        }
                                
                    }
                }
            }
        }
        return retval;
    }
    
    
    private Map< String, Map< UUID, Set< UUID > > > getActualFiles()
    {
        final Map< String, Map< UUID, Set< UUID > > > retval = new HashMap<>();
        for ( final File rootMember : toNonNull( m_root.listFiles() ) )
        {
            if ( rootMember.isDirectory() )
            {
                final String bucketName = rootMember.getName();
                retval.put( bucketName, new HashMap< UUID, Set< UUID > >() );
                for ( final File hashDir : toNonNull( rootMember.listFiles() ) )
                {
                    for ( final File objectDir : toNonNull( hashDir.listFiles() ) )
                    {

                        if ( objectDir.isDirectory() )
                        {
                            final UUID objectId = UUID.fromString( objectDir.getName() );
                            retval.get( bucketName ).put( objectId, new HashSet< UUID >() );
                            for ( final File blobFile : toNonNull( objectDir.listFiles() ) )
                            {
                                if ( !blobFile.isFile() )
                                {
                                    throw new RuntimeException( "Blob entry shoulda been a file: "
                                            + blobFile );
                                }
                                if (blobFile.getName().matches("[0-9a-fA-F-]*") )
                                {
                                retval.get( bucketName ).get( objectId )
                                        .add( UUID.fromString( blobFile.getName() ) );
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return retval;
    }
    
    
    private File [] toNonNull( final File [] array )
    {
        if ( null == array )
        {
            return NULL_FILE_ARRAY;
        }
        return array;
    }
    
    
    private final Pool m_pool;
    private final File m_root;
    private final BeansServiceManager m_serviceManager;
    
    private final static File [] NULL_FILE_ARRAY = new File[ 0 ];
}
