/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.frmwrk;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.target.Md5ComputingFileInputStream;
import com.spectralogic.util.exception.BlobReadFailedException;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.security.FastMD5;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

abstract class BasePublicCloudConnection extends BaseShutdownable implements PublicCloudConnection
{
    protected BasePublicCloudConnection()
    {
        initWorkPool();
    }
    
    
    final protected void setEndPoint( final String endPoint )
    {
        Validations.verifyNotNull( "End point", endPoint );
        m_endPoint = endPoint;
    }
    
    
    private String getObjectIndex( final S3Object o )
    {
        return getObjectIndex( o.getName() );
    }
    
    
    private String getObjectIndex( final String objectName )
    {
        return INDEX + SEPARATOR + objectName;
    }
    
    
    private String getObjectRoot( final UUID objectId )
    {
        return DATA + SEPARATOR + objectId;
    }
    
    
    private String getObjectMetadata( final S3Object o )
    {
        return getObjectMetadata( o.getId() );
    }
    
    
    private String getObjectMetadata( final UUID objectId )
    {
        return META + SEPARATOR + objectId + META_SUFFIX;
    }
    
    
    private String getBlobRoot( final S3Object o, final Blob blob )
    {
        return getBlobRoot( o.getId(), blob.getId() );
    }
    
    
    private String getBlobRoot( final UUID objectId, final UUID blobId )
    {
        return getObjectRoot( objectId ) + SEPARATOR + BLOB_PREFIX + blobId;
    }
    
    
    private String getBlobPart( final S3Object o, final Blob blob, final int partNumber )
    {
        return getBlobPart( o.getId(), blob.getId(), partNumber );
    }
    
    
    private String getBlobPart( final UUID objectId, final UUID blobId, final int partNumber )
    {
        return getBlobRoot( objectId, blobId ) + getPartNumberSuffix( partNumber );
    }
    
    
    private static String getPartNumberSuffix( final int partNumber )
    {
        String retval = String.valueOf( partNumber );
        while ( 5 > retval.length() )
        {
            retval = "0" + retval;
        }
        
        return "." + retval;
    }

    final public boolean isBlobAvailableOnCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob )
    {
        return new OperationRunner< Boolean >( "determine if blob " + blob.getId() + " is online" )
        {
            @Override
            public Boolean performOperation() {
                final Map< String, Long > sizes;
                try
                {
                    sizes = getBlobPartsOnCloud( cloudBucket.getName(), getBlobRoot( object, blob ) );
                    validateBlobParts( blob.getLength(), blob.getId(), sizes );
                }
                catch ( final Exception ex )
                {
                    if ( ex instanceof BlobReadFailedException){
                        markBlobAsSuspect( blob.getId());
                        return Boolean.FALSE;
                    } else {
                        throw new RuntimeException(
                                "Failed to get cloud blob parts for blob " + blob.getId() + ".", ex );
                    }
                }
                return Boolean.TRUE;
            }
        }.run( Level.WARN ).booleanValue();
    }

    final public boolean isBlobReadyToBeReadFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob )
    {
        return new OperationRunner< Boolean >( "determine if blob " + blob.getId() + " is online" )
        {
            @Override
            public Boolean performOperation() {
                final Map< String, Long > sizes;
                List< String > cloudBlobParts;
                try
                {
                    sizes = getBlobPartsOnCloud( cloudBucket.getName(), getBlobRoot( object, blob ) );
                    cloudBlobParts = validateBlobParts( blob.getLength(), blob.getId(), sizes );
                }
                catch ( final Exception ex )
                {
                    if ( ex instanceof BlobReadFailedException){
                        return Boolean.FALSE;
                    } else {
                        throw new RuntimeException(
                                "Failed to get cloud blob parts for blob " + blob.getId() + ".", ex );
                    }
                }
                try
                {
                    for ( final String cloudBlobPart : cloudBlobParts )
                    {
                        if ( !isBlobPartReadyToBeReadFromCloud( cloudBucket.getName(), cloudBlobPart ) )
                        {
                            return Boolean.FALSE;
                        }
                    }
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
                return Boolean.TRUE;
            }
        }.run( Level.WARN ).booleanValue();
    }


    protected List< String > validateBlobParts( long blobLength, UUID blobId, final Map< String, Long > sizes )  {
        final List< String > cloudBlobParts = new ArrayList<>( sizes.keySet() );
        Collections.sort( cloudBlobParts );

        long total = 0;
        boolean foundEnd = false;
        for ( final String part : new ArrayList<>( cloudBlobParts ) )
        {
            if ( foundEnd )
            {
                cloudBlobParts.remove( part );
            }
            else
            {
                total += sizes.get( part ).longValue();
                if ( total == blobLength )
                {
                    foundEnd = true;
                }
            }
        }
        if ( total != blobLength )
        {
            LOG.error( "Failed to compute blob parts in cloud due to length mismatch.  Blob length is "
                    + blobLength + ", but the sum of the cloud blob part lengths was " + total
                    + "." );
            throw new BlobReadFailedException(blobId,  "Failed to compute blob parts in cloud due to length mismatch.  Blob length is "
                    + blobLength + ", but the sum of the cloud blob part lengths was " + total
                    + ".");
        }

        return cloudBlobParts;
    }


    abstract protected boolean isBlobPartReadyToBeReadFromCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart ) throws Exception;

    abstract protected void markBlobAsSuspect(
            final UUID blobId) ;

    public void beginStagingToRead(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final int stagedDataExpirationInDays )
    {
        new VoidOperationRunner(
                "begin staging blob " + blob.getId()
                + " (" + new BytesRenderer().render( blob.getLength() ) + ")" )
        {
            @Override
            public void performVoidOperation() throws Exception
            {
                final Map< String, Long > sizes =
                        getBlobPartsOnCloud( cloudBucket.getName(), getBlobRoot( object, blob ) );
                final List< String > cloudBlobParts = validateBlobParts( blob.getLength(), blob.getId(), sizes );
                for ( final String cloudBlobPart : cloudBlobParts )
                {
                    beginStagingBlobPartToRead(
                            cloudBucket.getName(), cloudBlobPart, stagedDataExpirationInDays );
                }
            }
        }.run( Level.WARN );
    }


    abstract protected void beginStagingBlobPartToRead(
            final String cloudBucketName,
            final String cloudKeyBlobPart,
            final int stagedDataExpirationInDays ) throws Exception;


    final public List< Future< ? > > readBlobFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final File fileInCache )
    {
        final List< Future< ? > > writeThreads = new ArrayList<>();
        new VoidOperationRunner(
                "start threads to read blob " + blob.getId() + " (" +
                 new BytesRenderer().render( blob.getLength() ) + ")" )
        {
            @Override
            public void performVoidOperation() throws Exception {
                final Map< String, Long > sizes;
                final List< String > cloudBlobParts;
                try
                {
                    sizes = getBlobPartsOnCloud( cloudBucket.getName(), getBlobRoot( object, blob ) );
                    cloudBlobParts = validateBlobParts( blob.getLength(),blob.getId(), sizes );
                }
                catch ( final Exception ex )
                {
                    if ( ex instanceof BlobReadFailedException )
                    {
                        markBlobAsSuspect( blob.getId() );
                    }
                    throw new RuntimeException(
                            "Failed to get cloud blob parts for blob " + blob.getId() + ".", ex );
                }

                writeThreads.addAll( readBlobPartsFromCloud( cloudBucket, fileInCache, blob, cloudBlobParts, sizes ) );
            }
        }.run( Level.WARN );
        return writeThreads;
    }


    /**
     * @return {@code Map <cloud blob part key, length of cloud blob part key in bytes>}
     */
    abstract protected Map< String, Long > getBlobPartsOnCloud(
            final String cloudBucketName,
            final String cloudKeyBlobRoot ) throws Exception;


    private List< Future< ? > > readBlobPartsFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final File fileInCache,
            final Blob blob,
            final List< String > cloudBlobParts,
            final Map< String, Long > sizes )
    {
        final List< Future< ? > > writeThreads = new ArrayList<>();
        final BytesRenderer bytesRenderer = new BytesRenderer();
        long offset = 0;

        try
        {
            for ( final String cloudBlobPart : cloudBlobParts )
            {
                final long currentOffset = offset;
                writeThreads.add( s_wp.submit( new Runnable()
                {
                    final Duration duration = new Duration();

                    @Override
                    public void run()
                    {
                        int retries = 0;
                        boolean complete = false;

                        final long size = sizes.get( cloudBlobPart ).longValue();
                        TargetLogger.LOG.info(
                                "Reading blob part " + cloudBlobPart + " ("
                                        + bytesRenderer.render( size ) + ") from cloud..." );
                        try( final RandomAccessFile fos = new RandomAccessFile( fileInCache , "rw") )
                        {
                            final FileChannel channel = fos.getChannel();
                            final byte[] buffer = new byte[ 1024 * 1024 ];
                            while ( !complete )
                            {
                                final FastMD5 digest = new FastMD5();
                                try ( final InputStream in =
                                		readBlobPartFromCloud( cloudBucket.getName(), cloudBlobPart ) )
                                {
                                    long writtenCount = 0;
                                    while ( true )
                                    {
                                        final int count = in.read( buffer );
                                        if ( 0 > count )
                                        {
                                            break;
                                        }

                                        try
                                        {
                                            FileLock lock = null;
                                            lock = channel.lock( currentOffset + writtenCount, count, false );
                                            channel.write( ByteBuffer.wrap( buffer, 0, count ),
                                            		currentOffset + writtenCount );
                                            lock.release();
                                            digest.update( buffer, 0, count );
                                        } catch ( final Exception ex )
                                        {
                                            throw new RuntimeException( ex );
                                        }

                                        writtenCount += count;
                                    }
                                }
                                catch ( final IOException e )
								{
                                	if ( retries < MAX_TRANSFER_RETRIES )
                                	{
                                		try
										{
											Thread.sleep( MILLISECONDS_BETWEEN_RETRIES * retries );
										}
										catch ( InterruptedException e1 )
										{
											throw new RuntimeException( e1 );
										}
                                		retries++;
                                	}
                                	else
                                	{
                                		throw new RuntimeException( "Max retries exceeded to read blob part "
                                				+ cloudBlobPart, e );
                                	}
								}

                                verifyNoCorruption(
                                        digest,
                                        getBlobPartWrittenToCloudMd5Base64( cloudBucket.getName(), cloudBlobPart ),
                                        blob,
                                        size );

                                TargetLogger.LOG.info(
                                        "Finished reading blob part " + cloudBlobPart + " ("
                                                + bytesRenderer.render( size ) + ") from cloud at "
                                                + bytesRenderer.render( size, duration ) + "." );

                                complete = true;
                            }
                        }
                        catch ( final IOException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }
                }));
                offset += sizes.get( cloudBlobPart );
            }
        } catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }

        return writeThreads;
    }


    /**
     * @return the {@link InputStream} for the binary data corresponding to the designated blob part
     */
    abstract protected InputStream readBlobPartFromCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart );


    public final List< Future< ? > > writeBlobToCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final long objectSize,
            final Blob blob,
            final int numBlobsForObject,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata,
            final long maxBlobPartLength,
            final Object initialDataPlacement )
    {
        final List< Future< ? > > writeThreads = new ArrayList<>();
        new VoidOperationRunner(
                "start threads to write blob " + blob.getId() + " (" +
                 new BytesRenderer().render( blob.getLength() ) + ")" )
        {
            @Override
            public void performVoidOperation()
            {
                if ( null != objectCreationDate )
                {
                    final boolean objectMetadataWritten;
                    try
                    {
                        objectMetadataWritten =
                                isObjectMetadataWritten( cloudBucket.getName(), getObjectMetadata( object ) );
                    }
                    catch ( final IllegalArgumentException ex )
                    {
                        throw new RuntimeException(
                                ex.getMessage(), ex );
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException(
                                "Failed to determine if object metadata is written.", ex );
                    }

                    if ( !objectMetadataWritten )
                    {
                        writeObjectMetadataAndIndex(
                                cloudBucket, object, blob, numBlobsForObject, objectCreationDate, metadata, initialDataPlacement );
                    }
                }

                writeThreads.addAll( writeBlobPartsToCloud(
                        cloudBucket, object, blob, fileInCache, maxBlobPartLength, initialDataPlacement ) );
            }
        }.run( Level.WARN );

        return writeThreads;
    }


    private void writeObjectMetadataAndIndex(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final int numBlobsForObject,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata,
            final Object initialDataPlacement)
    {
        TargetLogger.LOG.info( "Writing object " + object.getId() + "'s metadata and index..." );
        try
        {
            writeObjectMetadata(
                    cloudBucket.getName(),
                    getObjectMetadata( object ),
                    computeObjectMetadata( object, objectCreationDate, numBlobsForObject, metadata ),
                    initialDataPlacement );
            TargetLogger.LOG.info( "Wrote object metadata successfully." );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException(
                    "Failed to write object metadata for " + object + ".", ex );
        }

        try
        {
            writeObjectIndex(
                    cloudBucket.getName(),
                    getObjectIndex( object ),
                    computeObjectIndexMetadata( object, blob, numBlobsForObject ),
                    initialDataPlacement );
            TargetLogger.LOG.info( "Wrote object index successfully." );
        }
        catch ( final Exception ex )
        {
            LOG.warn( "Cannot write object " + object.getId() + " (" + object.getName()
                      + ") to index.", ex );
        }
    }


    private ByteArrayInputStream computeObjectMetadata(
            final S3Object object,
            final Date objectCreationDate,
            final int numBlobsForObject,
            final Set< S3ObjectProperty > metadata )
    {
        final ByteArrayOutputStream retval = new ByteArrayOutputStream();
        final Properties pMetadata = new Properties();
        pMetadata.put( NameObservable.NAME, object.getName() );
        pMetadata.put( KeyValueObservable.CREATION_DATE, String.valueOf( objectCreationDate.getTime() ) );
        pMetadata.put( KeyValueObservable.TOTAL_BLOB_COUNT, String.valueOf( numBlobsForObject ) );
        for ( final S3ObjectProperty m : metadata )
        {
            pMetadata.put( m.getKey(), m.getValue() );
        }
        try
        {
            pMetadata.store( retval, null );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }

        return new ByteArrayInputStream( retval.toByteArray() );
    }


    private Map< String, String > computeObjectIndexMetadata(
            final S3Object object,
            final Blob blob,
            final int numBlobsForObject )
    {
        final Map< String, String > retval = new HashMap<>();
        retval.put( OBJECT_ID, object.getId().toString() );
        if ( 1 == numBlobsForObject )
        {
            retval.put( SIMPLE_PATH_DATA, getBlobPart( object, blob, 0 ) );
        }
        retval.put( SIMPLE_PATH_META, getObjectMetadata( object ) );
        return retval;
    }


    abstract protected boolean isObjectMetadataWritten(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata ) throws Exception;


    /**
     * @param metadata - The binary data corresponding to a key-value properties file containing all relevant
     * properties of the {@link S3Object} as well as any {@link S3ObjectProperty}s
     */
    abstract protected void writeObjectMetadata(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata,
            final ByteArrayInputStream metadata,
            final Object initialDataPlacement ) throws Exception;


    /**
     * Implementations of this method are not required to successfully write an object index.  For example,
     * it is possible that the <code>cloudKeyObjectIndex</code> passed to this method is invalid for the
     * implementation of this method due to restrictions by the cloud provider.
     *
     * @param metadata - Custom metadata that should be written as metadata on the cloud object written for
     * the object index
     */
    abstract protected void writeObjectIndex(
            final String cloudBucketName,
            final String cloudKeyObjectIndex,
            final Map< String, String > metadata,
            final Object initialDataPlacement ) throws Exception;


    private List< Future< ? > > writeBlobPartsToCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final File fileInCache,
            final long maxBlobPartLength,
            final Object initialDataPlacement )
    {
        final List< Future< ? > > writeThreads = new ArrayList<>();
        final BytesRenderer bytesRenderer = new BytesRenderer();
        long remaining = blob.getLength();
        int partNumber = -1;
        while ( 0 < remaining || -1 == partNumber )
        {
            try
            {
                ++partNumber;
                final int threadPartNumber = partNumber;
                final int number = partNumber;
                final long offset = maxBlobPartLength * partNumber;
                final long length = Math.min( remaining, maxBlobPartLength );
                remaining -= length;

                writeThreads.add( s_wp.submit( new Runnable() {
                    final Duration duration = new Duration();

                    @Override
                    public void run()
                    {
                    	int retries = 0;
                        TargetLogger.LOG.info(
                                "Writing blob part " + getBlobPart( object, blob, threadPartNumber ) + " ("
                                        + bytesRenderer.render( length ) + ") to cloud..." );
                        boolean complete = false;
                        while ( !complete )
                        {
                            try ( final Md5ComputingFileInputStream fis = constructFis( fileInCache ) )
                            {
                                final InputStream rangedInputStream = new BufferedInputStream(new RangedInputStream( fis, offset, length ));

                                writeBlobPartToCloud(
                                        cloudBucket.getName(),
                                        getBlobPart( object, blob, number ),
                                        blob,
                                        rangedInputStream,
                                        length,
                                        initialDataPlacement );
                                verifyNoCorruption(
                                        fis.getDigest(),
                                        getBlobPartWrittenToCloudMd5Base64(
                                                cloudBucket.getName(), getBlobPart( object, blob, number ) ),
                                        blob,
                                        length );
                                TargetLogger.LOG.info(
                                        "Wrote blob part " + getBlobPart( object, blob, number ) + " ("
                                                + bytesRenderer.render( length ) + ") to cloud at "
                                                + bytesRenderer.render( length, duration ) + "." );
                                complete = true;
                            } catch ( final CloudTransferFailedException e )
                            {
                            	if ( retries < MAX_TRANSFER_RETRIES )
                            	{
                            		try
									{
										Thread.sleep( MILLISECONDS_BETWEEN_RETRIES * retries );
									}
									catch ( InterruptedException e1 )
									{
										throw new RuntimeException( e1 );
									}
                            		retries++;
                            	}
                            	else
                            	{
                            		throw new RuntimeException( "Max retries exceeded to write blob part "
                            				+ getBlobPart( object, blob, number ), e );
                            	}
                            } catch ( final IOException fileStreamException )
							{
								throw new RuntimeException( fileStreamException );
							}
                        }
                    }
                } ) );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException(
                        "Failed to write blob " + getBlobRoot( object, blob ) + " to cloud.", ex );
            }
        }

        return writeThreads;
    }


    private void verifyNoCorruption(
            final FastMD5 digest,
            final String actualMd5Digest,
            final Blob blob,
            final long partLength )
    {
        final String expectedMd5Digest = Base64.encodeBase64String( digest.digestAndReset() );
        if (actualMd5Digest == null) {
            LOG.warn("MD5 digest from cloud could not be used to verify data. It likely has had an etag changed due" +
                    " to being restored from the target provider's offline storage.");
        } else {
            if ( !expectedMd5Digest.equals( actualMd5Digest ) )
            {
                markBlobAsSuspect(blob.getId());
                throw new RuntimeException(
                        "Corruption occurred during transmission.  Expected MD5 checksum to be "
                                + expectedMd5Digest + ", but was " + actualMd5Digest + "." );
            }
        }
        if ( !ChecksumObservable.CHECKSUM_VALUE_NOT_COMPUTED.equals( blob.getChecksum() )
        		&& partLength == blob.getLength() && ChecksumType.MD5.equals( blob.getChecksumType() ) )
        {
            if ( !blob.getChecksum().equals( expectedMd5Digest ) )
            {
                markBlobAsSuspect(blob.getId());
                throw new RuntimeException(
                        "Corruption occured prior to transmission.  Expected MD5 checksum to be "
                        + expectedMd5Digest + ", but the blob reports it should have a checksum of "
                        + blob.getChecksum() + "." );
            }
        }
    }


    abstract protected void writeBlobPartToCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart,
            final Blob blob,
            final InputStream inputStream,
            final long length,
            final Object initialDataPlacement ) throws CloudTransferFailedException;


    /**
     * @return the MD5 of the specified blob part according to the cloud provider
     */
    abstract protected String getBlobPartWrittenToCloudMd5Base64(
            final String cloudBucketName,
            final String cloudKeyBlobPart );


    private Md5ComputingFileInputStream constructFis(final File file )
    {
        try
        {
            return new Md5ComputingFileInputStream( file );
        }
        catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    final protected Map< String, String > toBeanPropsMap(
            final SimpleBeanSafeToProxy bean )
    {
        final Map< String, String > retval = new HashMap<>();
        for ( final String prop : BeanUtils.getPropertyNames( bean.getClass() ) )
        {
            try
            {
                final Object rawValue = BeanUtils.getReader( bean.getClass(), prop ).invoke( bean );
                if ( null == rawValue )
                {
                    throw new RuntimeException( "Property " + prop + " cannot have a null value." );
                }
                retval.put(
                        prop,
                        MarshalUtil.getStringFromTypedValue( rawValue ) );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }

        return retval;
    }


    final public BucketOnPublicCloud discoverContents(
            final PublicCloudBucketInformation cloudBucket,
            final String marker )
    {
        return new OperationRunner< BucketOnPublicCloud >(
                "read contents for bucket " + cloudBucket.getName() + " starting at marker " + marker )
        {
            @Override
            protected BucketOnPublicCloud performOperation() throws Exception
            {
                final BucketOnPublicCloud retval = BeanFactory.newBean( BucketOnPublicCloud.class );

                final PublicCloudBucketInformation bucketInformation =
                        getExistingBucketInformation( cloudBucket.getName() );
                retval.setBucketName( bucketInformation.getLocalBucketName() );

                final Map< UUID, Set< Exception > > failures = new HashMap<>();
                final ContentsSegment segment = discoverContentsSegment( cloudBucket.getName(), marker );
                retval.setNextMarker( segment.getNextMarker() );
                retval.setObjects( CollectionFactory.toArray(
                        S3ObjectOnMedia.class,
                        discoverContentsInternal( cloudBucket, segment, failures ) ) );
                retval.setFailures( failures );

                return retval;
            }
        }.run( Level.WARN );
    }


    private Set< S3ObjectOnMedia > discoverContentsInternal(
            final PublicCloudBucketInformation cloudBucket,
            final ContentsSegment segment,
            final Map< UUID, Set< Exception > > failures )
    {
        LOG.info( "Discovering contents of segment of public cloud bucket " + cloudBucket.getName() + " with "
                  + segment.getSegment().size() + " objects containing "
                  + segment.getBlobCount() + " blobs..." );

        final Set< S3ObjectOnMedia > retval =
                Collections.< S3ObjectOnMedia >synchronizedSet( new HashSet< S3ObjectOnMedia >() );
        final Set< Future< ? > > futures = new HashSet<>();
        for ( final Map.Entry< UUID, Set< UUID > > e : segment.getSegment().entrySet() )
        {
            futures.add( s_wp.submit( new DiscoverContentsWorker(
                    cloudBucket, e.getKey(), e.getValue(), retval, failures ) ) );
        }

        try
        {
            for ( final Future< ? > f : futures )
            {
                f.get( 1, TimeUnit.HOURS );
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }

        LOG.info( "Discovered contents of segment of public cloud bucket " + cloudBucket.getName() + " with "
                + segment.getSegment().size() + " objects containing "
                + segment.getBlobCount() + " blobs." );
        return retval;
    }


    private final class DiscoverContentsWorker implements Runnable
    {
        private DiscoverContentsWorker(
                final PublicCloudBucketInformation cloudBucket,
                final UUID objectId,
                final Set< UUID > blobIds,
                final Set< S3ObjectOnMedia > results,
                final Map< UUID, Set< Exception > > failures )
        {
            m_cloudBucket = cloudBucket;
            m_objectId = objectId;
            m_blobIds = blobIds;
            m_results = results;
            m_failures = failures;
        }


        public void run()
        {
            final Set< BlobOnMedia > boms = new HashSet<>();
            for ( final UUID blobId : m_blobIds )
            {
                final BlobOnMedia bom;
                try
                {
                    bom = MarshalUtil.newBean(
                            BlobOnMedia.class,
                            discoverBlobOnCloud(
                                    m_cloudBucket.getName(),
                                    getBlobPart( m_objectId, blobId, 0 ) ) );
                    final Map< String, Long > sizes = getBlobPartsOnCloud(
                            m_cloudBucket.getName(), getBlobRoot( m_objectId, blobId ) );
                    validateBlobParts( bom.getLength(), blobId, sizes );
                }
                catch ( final Exception ex )
                {
                    recordFailure( blobId, ex );
                    return;
                }
                bom.setId( blobId );
                boms.add( bom );
            }

            final Set< S3ObjectMetadataKeyValue > metadatas = new HashSet<>();
            final Map< String, String > oomPropValues = new HashMap<>();
            try ( final InputStream is = discoverObjectOnCloud(
                    m_cloudBucket.getName(), getObjectMetadata( m_objectId ) ) )
            {
                final Properties properties = new Properties();
                properties.load( is );
                for ( final String key : properties.stringPropertyNames() )
                {
                    if ( BeanUtils.getPropertyNames( S3Object.class ).contains( key ) )
                    {
                        oomPropValues.put(
                                ( NameObservable.NAME.equals( key ) ) ? S3ObjectOnMedia.OBJECT_NAME : key,
                                properties.getProperty( key ) );
                    }
                    else
                    {
                        metadatas.add(
                                BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                                .setKey( key ).setValue( properties.getProperty( key ) ) );
                    }
                }
            }
            catch ( final Exception ex )
            {
                recordFailure( null, ex );
                return;
            }

            final S3ObjectOnMedia oom = MarshalUtil.newBean( S3ObjectOnMedia.class, oomPropValues );
            oom.setId( m_objectId );
            oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, boms ) );
            oom.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
            m_results.add( oom );
        }


        private void recordFailure( final UUID blobId, final Exception ex )
        {
            final String blobSuffix = ( null == blobId ) ? "" : " (blob " + blobId + ")";
            LOG.warn( "Validation failed discovering object " + m_objectId + blobSuffix
                      + ".  Will skip its discovery.", ex );
            synchronized ( m_failures )
            {
                if ( !m_failures.containsKey( m_objectId ) )
                {
                    m_failures.put( m_objectId, new HashSet< Exception >() );
                }
                m_failures.get( m_objectId ).add( ex );
            }
        }


        private final PublicCloudBucketInformation m_cloudBucket;
        private final UUID m_objectId;
        private final Set< UUID > m_blobIds;
        private final Set< S3ObjectOnMedia > m_results;
        private final Map< UUID, Set< Exception > > m_failures;
    } // end inner class def


    /**
     * @return the {@link ContentsSegment} at the specified marker (if the specified marker is null, returns
     * the first {@link ContentsSegment} (contents to discover are always broken up into segments for
     * scalability purposes)
     */
    protected abstract ContentsSegment discoverContentsSegment(
            final String cloudBucketName,
            final String marker ) throws Exception;


    /**
     * @return {@code Map <property key, property value>} for properties of the {@link Blob}
     */
    protected abstract Map< String, String > discoverBlobOnCloud(
            final String cloudBucketName,
            final String cloudKeyBlobPart ) throws Exception;


    /**
     * @return an {@link InputStream} for a key-value properties file that contains all properties and custom
     * metadata for the {@link S3Object}
     */
    protected abstract InputStream discoverObjectOnCloud(
            final String cloudBucketName,
            final String cloudKeyObjectMetadata ) throws Exception;


    @Override
	final public void delete( final PublicCloudBucketInformation cloudBucket, final Set< S3ObjectOnMedia > objects )
    {
        new VoidOperationRunner(
                "delete objects: " + LogUtil.getShortVersion( objects, 10 ) )
        {
            @Override
            public void performVoidOperation() throws Exception
            {
                final Set< String > keysToDelete = new HashSet<>();
                for ( final S3ObjectOnMedia object : objects )
                {
                    try ( final InputStream is =
                            discoverObjectOnCloud( cloudBucket.getName(), getObjectMetadata( object.getId() ) ) )
                    {
                        final Properties properties = new Properties();
                        properties.load( is );
                        keysToDelete.add(
                                getObjectIndex( properties.getProperty( NameObservable.NAME ) ) );
                        keysToDelete.add( getObjectMetadata( object.getId() ) );
                        keysToDelete.addAll( getBlobPartsOnCloud(
                                cloudBucket.getName(), getObjectRoot( object.getId() ) ).keySet() );
                    }
                }

                deleteKeys( cloudBucket.getName(), keysToDelete );
            }
        }.run( Level.WARN );
    }


    /**
     * Deletes the specified cloud keys
     */
    protected abstract void deleteKeys(
            final String cloudBucketName,
            final Set< String > cloudKeys ) throws Exception;


    protected abstract class OperationRunner< T >
    {
        protected OperationRunner( final String operationDescription )
        {
            m_operationDescription = operationDescription;
            Validations.verifyNotNull( "Operation description", m_operationDescription );
        }


        final public T run( final Level failureLogLevel )
        {
            Validations.verifyNotNull( "End point", m_endPoint );

            final Duration duration = new Duration();
            TargetLogger.LOG.info( "On " + m_endPoint + ", attempting to " + m_operationDescription + "..." );
            try
            {
                final T retval = performOperation();
                final String msg =
                        "On " + m_endPoint + ", succeeded to " + m_operationDescription
                        + " in " + duration + ".";
                LOG.info( ( null == retval || 50 > retval.toString().length() ) ?
                        msg + "  Result: " + retval
                        : msg );
                TargetLogger.LOG.info( msg + "  Result:" + Platform.NEWLINE + ( ( null == retval ) ?
                        "null"
                        : LogUtil.getShortVersion( retval.toString() ) ) );
                return retval;
            }
            catch ( final Exception ex )
            {
                log( failureLogLevel,
                     "On " + m_endPoint + ", failed to " + m_operationDescription
                     + " after " + duration + ".", ex );
                throw getFailure( m_operationDescription, ex );
            }
        }


        protected abstract T performOperation() throws Exception;


        private final String m_operationDescription;
    } // end inner class def


    protected abstract class VoidOperationRunner extends OperationRunner< Object >
    {
        protected VoidOperationRunner( final String operationDescription )
        {
            super( operationDescription );
        }


        @Override
        protected Object performOperation() throws Exception
        {
            performVoidOperation();
            return null;
        }


        protected abstract void performVoidOperation() throws Exception;
    } // end inner class def


    /**
     * @return the failure translated from the passed-in raw failure and operation description
     */
    protected abstract RuntimeException getFailure( final String operationDescription, final Exception t );


    final protected void log( final Level level, final String message, final Throwable t )
    {
        if ( Level.INFO == level )
        {
            LOG.log( level, message );
        }
        else
        {
            LOG.log( level, message, t );
        }
        TargetLogger.LOG.log( level, message, t );
    }


    private static void initWorkPool()
    {
        synchronized ( WP_LOCK )
        {
            if ( null == s_wp || s_wp.isShutdown() )
            {
                s_wp = WorkPoolFactory.createWorkPool(
                        MAX_THREADS,
                        "PublicCloudConnectionWorker" );
            }
        }
    }


    public void syncUploads( final String cloudBucketName )
    {
    	//nothing to sync for this connection type
    }


    protected volatile String m_endPoint;

    private final static String META = "meta";
    private final static String META_SUFFIX = ".props";
    final static String SEPARATOR = "/";
    private final static String INDEX = "index";
    final static String BLOB_PREFIX = "blob.";
    final static String FIRST_BLOB_SUFFIX = getPartNumberSuffix( 0 );
    protected final static String DATA = "data";
    protected final static String OBJECT_ID = "objectid";
    protected final static String SIMPLE_PATH_DATA = "objectdata";
    protected final static String SIMPLE_PATH_META = "objectmeta";
    protected final static String SPECTRA_BUCKET_META_KEY = "spectra.blackpearl.bucket";
    protected final static ByteArrayInputStream ZERO_LENGTH_BYTE_ARRAY_IS =
            new ByteArrayInputStream( (byte[])Array.newInstance( byte.class, 0 ) );

    private static WorkPool s_wp;
    private final static Object WP_LOCK = new Object();

    protected final static Logger LOG = Logger.getLogger( BasePublicCloudConnection.class );


    private final static int MAX_TRANSFER_RETRIES = 10;
    private final static int MILLISECONDS_BETWEEN_RETRIES = 5000;
}
