/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.taperesource;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.zip.CRC32;

import com.spectralogic.s3.common.dao.domain.tape.DriveTestResult;
import com.spectralogic.s3.common.rpc.tape.domain.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.spectralogic.util.security.ChecksumType;

import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.render.BytesRenderer;

public class SimTapeDriveResource extends BaseRpcResource implements TapeDriveResource
{
    SimTapeDriveResource(
            final String driveSerialNumber,
            final SimStateManager stateManager )
    {
        m_stateManager = stateManager;
        m_maxBytesPerSecond = stateManager.getSimulatorConfig().getMaxBytesPerSecond();

        m_driveSerialNumber = driveSerialNumber;
        m_rootFilesystem = 
                m_stateManager.getSimulatorConfig().getVirtualLibraryPath();
        if ( null == stateManager.getDrive( m_driveSerialNumber ) )
        {
            throw new IllegalStateException( "Tape drive does not exist: " + m_driveSerialNumber );
        }
    }
    
    
    public RpcFuture< TapeDriveInformation > getDriveInformation()
    {
        final SimDrive drive = getDrive();
        final TapeDriveInformation retval = BeanFactory.newBean( TapeDriveInformation.class );
        BeanCopier.copy( retval, drive );
        
        return new RpcResponse<>( retval );
    }
    
    
    public RpcFuture< BlobIoFailures > verifyData( final S3ObjectsToVerify objects )
    {

        final Set< BlobIoFailure > failures = new HashSet<>();

        m_stateManager.simulateDelay( m_ioDelay );
        for ( final S3ObjectOnMedia object : objects.getBuckets()[ 0 ].getObjects() )
        {
            BlobOnMedia[] blobs = object.getBlobs();
            for (BlobOnMedia blob : blobs) {
                final String bucketName = objects.getBuckets()[ 0 ].getBucketName();
                final File fileOnTape = getTapeFile( bucketName, object.getObjectName() + "." + blob.getOffset() );
                if ( !fileOnTape.exists() )
                {
                    failures.add(BeanFactory.newBean( BlobIoFailure.class )
                            .setBlobId( blob.getId() )
                            .setFailure( BlobIoFailureType.DOES_NOT_EXIST )) ;
                }
                else
                {
                    // Validate checksum if the file exists
                    final BlobIoFailure checksumFailure = validateBlobChecksum( 
                            bucketName, object.getObjectName(), blob );
                    if ( checksumFailure != null )
                    {
                        failures.add( checksumFailure );
                    }
                }
            }

        }

        final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class )
                .setFailures( CollectionFactory.toArray( BlobIoFailure.class, failures ) );
        return new RpcResponse<>( retval );
    }


    public RpcFuture< String > openDs3Contents( 
            final boolean includeObjectMetadata, 
            final boolean recursive )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing." );
    }


    public RpcFuture< String > openForeignContents( 
            final String bucketName,
            final String blobCountMetadataKey,
            final String creationDateMetadataKey,
            final long maxBlobSize,
            final long maxLtfsExtendedAttributeValueLengthInBytesToIncludeInObjectMetadata )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing." );
    }
    
    
    public RpcFuture< S3ObjectsOnMedia > readContents( 
            final String handle, 
            final int maxResults,
            final long maxLength )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing." );
    }
    

    public RpcFuture< ? > closeContents( final String handle )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing." );
    }

    
    public RpcFuture< LoadedTapeInformation > getLoadedTapeInformation()
    {
        final SimTape tape = getTape();
        if ( null == tape )
        {
            return null;
        }
        verifyTapeNotPreparedForRemoval();
        verifyTapeQuiesced();
        
        final LoadedTapeInformation retval = 
                BeanFactory.newBean( LoadedTapeInformation.class );
        BeanCopier.copy( retval, tape );
        
        final String tapeId = readProperty( ".tapeid" );
        retval.setTapeId( ( null == tapeId ) ? null : UUID.fromString( tapeId ) );
        retval.setSerialNumber( getDrive().getTapeSerialNumber() );
        
        return new RpcResponse<>( retval );
    }
    
    
    public RpcFuture< String > getLoadedTapeSerialNumber()
    {
        return new RpcResponse<>( getLoadedTapeInformation().getWithoutBlocking().getSerialNumber() );
    }

    
    public RpcFuture< FormattedTapeInformation > getFormattedTapeInformation()
    {
        final SimTape tape = getTape();
        if ( null == tape )
        {
            return null;
        }
        verifyTapeNotPreparedForRemoval();
        verifyTapeQuiesced();
        
        final FormattedTapeInformation retval = BeanFactory.newBean( FormattedTapeInformation.class );
        BeanCopier.copy( retval, tape );
        
        final String tapeId = readProperty( ".tapeid" );

        retval.setTapeId( ( null == tapeId ) ? null : UUID.fromString( tapeId ) );
        retval.setSerialNumber( getDrive().getTapeSerialNumber() );

        final String loaded = readProperty( ".loaded" );
        if (!Boolean.parseBoolean(loaded)) {
            m_stateManager.simulateDelay(m_stateManager.getSimulatorConfig().getLoadDelay(), "load on drive: " + m_driveSerialNumber);
            writeProperty(".loaded", "true");
        }

        final String firstLocate = readProperty( ".firstLocate" );
        if (firstLocate == null) {
            LOG.info("Performing first locate on tape " + tape.getBarCode() + ", no delay will be simulated.");
            writeProperty(".firstLocate", new Date().toString());
        } else {
            m_stateManager.simulateDelay( m_stateManager.getSimulatorConfig().getLocateDelay(), "locate on drive: " + m_driveSerialNumber );
        }

        return new RpcResponse<>( retval );
    }
    

    private boolean tapeIsFormatted() {
        final String formatted = readProperty( ".formatted" );
        return Boolean.parseBoolean(formatted);
    }


    public RpcFuture< ? > prepareForRemoval()
    {
        if ( getTape().isRemovalPrepared() )
        {
            return null;
        }
        if ( tapeIsFormatted() && !getTape().isContainsForeignData() )
        {
            quiesce();
        }

        writeProperty(".loaded", "false");
        m_stateManager.simulateDelay(m_stateManager.getSimulatorConfig().getUnloadDelay(), "unload on drive " + m_driveSerialNumber );
        getTape().setRemovalPrepared( true );
        
        return null;
    }

    
    public RpcFuture< BlobIoFailures > readData( final S3ObjectsIoRequest objects )
    {
        validateCanPerformIo();

        final Set< BlobIoFailure > failures = new HashSet<>();

        m_stateManager.simulateDelay( m_ioDelay );
        for ( final S3ObjectIoRequest object : objects.getBuckets()[ 0 ].getObjects() )
        {
            failures.addAll( readDataInternal(
                    objects.getCacheRootPath(),
                    objects.getBuckets()[ 0 ].getBucketName(),
                    object ) );
        }

        final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class )
                .setFailures( CollectionFactory.toArray( BlobIoFailure.class, failures ) );
        return new RpcResponse<>( retval );
    }

    
    private Set< BlobIoFailure > readDataInternal(
            final String cacheRootPath,
            final String bucketName, 
            final S3ObjectIoRequest object )
    {
        final Set< BlobIoFailure > failures = new HashSet<>();
        for ( final BlobIoRequest blob : object.getBlobs() )
        {
            final BlobIoFailure failure = readDataInternal( 
                    cacheRootPath,
                    bucketName, 
                    object.getObjectName(),
                    blob );
            if ( null != failure )
            {
                failures.add( failure );
            }
        }
        
        return failures;
    }

    
    private BlobIoFailure readDataInternal(
            final String cacheRootPath,
            final String bucketName, 
            final String objectName, 
            final BlobIoRequest blob )
    {
        final File fileOnTape = getTapeFile( bucketName, objectName + "." + blob.getOffset() );
        final File fileOnCache = new File( cacheRootPath + blob.getFileName() );
        
        if ( !fileOnTape.exists() )
        {
            return BeanFactory.newBean( BlobIoFailure.class )
                    .setBlobId( blob.getId() )
                    .setFailure( BlobIoFailureType.DOES_NOT_EXIST );
        }
        try
        {
            Files.copy( fileOnTape.toPath(), fileOnCache.toPath() );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException(
                    "Failed to copy " + fileOnTape + " to " + fileOnCache + ".", ex );
        }

        LOG.info( fileOnTape.getName() + " has been read from simulated tape to cache." );
        return null;
    }

    
    public RpcFuture< BlobIoFailures > writeData(
            final LtfsFileNamingMode ltfsFileNamingMode,
            final S3ObjectsIoRequest objects )
    {
        final long startTime = System.currentTimeMillis();
        long minDelayTime = 0;

        validateCanPerformIo();
        final Set< BlobIoFailure > failures = new HashSet<>();
        if ( m_maxBytesPerSecond != null ) {
            final long totalSize = Arrays.stream(objects.getBuckets()).map(b -> {
                return Arrays.stream(b.getObjects()).map(o -> {
                    return Arrays.stream(o.getBlobs()).map(bi -> {
                        return new File(objects.getCacheRootPath() + bi.getFileName()).length();
                    }).reduce(0L, Long::sum);
                }).reduce(0L, Long::sum);
            }).reduce(0L, Long::sum);
            LOG.info("Will write " + new BytesRenderer().render(totalSize) + " at no more than " + m_maxBytesPerSecond + " bytes per second");
            minDelayTime = totalSize * 1000 / m_maxBytesPerSecond;
        }

        m_stateManager.simulateDelay( m_ioDelay );
        for ( final S3ObjectIoRequest object : objects.getBuckets()[ 0 ].getObjects() )
        {
            failures.addAll( writeDataInternal(
                    objects.getCacheRootPath(),
                    objects.getBuckets()[ 0 ].getBucketName(),
                    object ) );
        }

        final long timeTakenSoFar = System.currentTimeMillis() - startTime;
        final long timeToDelay = minDelayTime - timeTakenSoFar;
        if (timeToDelay > 0) {
            LOG.info("Time taken writing so far " + timeTakenSoFar + "ms");
            m_stateManager.simulateDelay((int) timeToDelay, "remaining write delay.");
        }

        final BlobIoFailures retval = BeanFactory.newBean( BlobIoFailures.class )
                .setFailures( CollectionFactory.toArray( BlobIoFailure.class, failures ) );
        writeProperty( ".checkpoint", UUID.randomUUID().toString() );
        return new RpcResponse<>( retval );
    }  

    
    private Set< BlobIoFailure > writeDataInternal(
            final String cacheRootPath,
            final String bucketName, 
            final S3ObjectIoRequest object )
    {
        final Set< BlobIoFailure > failures = new HashSet<>();
        for ( final BlobIoRequest blob : object.getBlobs() )
        {
            final BlobIoFailure failure = writeDataInternal( 
                    cacheRootPath,
                    bucketName, 
                    object.getObjectName(),
                    blob );
            if ( null != failure )
            {
                failures.add( failure );
            }
        }
        
        return failures;
    }

    
    private BlobIoFailure writeDataInternal(
            final String cacheRootPath,
            final String bucketName, 
            final String objectName, 
            final BlobIoRequest blob )
    {
        final SimTape tape = getTape();
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final File fileOnTape = getTapeFile( bucketName, objectName + "." + blob.getOffset() );
        final File fileOnCache = new File( cacheRootPath + blob.getFileName() );
        
        if ( !fileOnCache.exists() )
        {
            throw new IllegalStateException( 
                    "Object " + blob.getFileName() + " wasn't in the cache." );
        }
        final long logicalSize = fileOnCache.length();
        final long physicalSize = (long)( ( new SecureRandom().nextDouble() + 0.1 ) * logicalSize );
        if (m_maxBytesPerSecond == null) {
            LOG.info( "In writing object " + fileOnTape.getName() + ", "
                    + bytesRenderer.render( logicalSize ) + " will consume "
                    + bytesRenderer.render( physicalSize ) + " on simulated tape." );
        } else {
            LOG.info( "In writing object " + fileOnTape.getName() + ", "
                    + bytesRenderer.render( logicalSize ) + " will write "
                    + bytesRenderer.render( physicalSize ) + " to null output stream." );
        }

        if ( physicalSize > tape.getAvailableRawCapacity() )
        {
            return BeanFactory.newBean( BlobIoFailure.class )
                    .setFailure( BlobIoFailureType.OUT_OF_SPACE )
                    .setBlobId( blob.getId() );
        }
        
        try
        {
            if (m_maxBytesPerSecond == null) {
                if (!fileOnTape.exists()) {
                    Files.copy( fileOnCache.toPath(), fileOnTape.toPath());
                } else {
                    String newName = fileOnTape.getName() + "_" + UUID.randomUUID().toString();
                    File newFile = new File(fileOnTape.getParent(), newName);
                    Files.copy( fileOnCache.toPath(), newFile.toPath());
                }

            } else {
                Files.copy( fileOnCache.toPath(), OutputStream.nullOutputStream() );
            }
            final ChecksumType checksumType = blob.getChecksumType() != null ? blob.getChecksumType() : ChecksumType.MD5;
            final String checksum = blob.getChecksum();
            saveChecksumMetadata( bucketName, objectName, blob.getOffset(), checksum, checksumType );

            LOG.info( "Calculated " + checksumType.getAlgorithmName() + " checksum for " + fileOnTape.getName() + ": " + checksum );

            getTape().setQuiesceRequired( true );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        
        tape.setAvailableRawCapacity( tape.getAvailableRawCapacity() - physicalSize );
        LOG.info( fileOnTape.getName() + " has been written to simulated tape." );
        return null;
    }

    
    /**
     * Saves checksum metadata to a file alongside the blob data.
     */
    private void saveChecksumMetadata(
            final String bucketName,
            final String objectName,
            final long offset,
            final String checksum,
            final ChecksumType checksumType ) throws IOException
    {
        final File metadataFile = getTapeFile( bucketName, objectName + "." + offset + ".checksum" );
        try ( final PrintWriter pw = new PrintWriter( metadataFile ) )
        {
            pw.println( "checksumType=" + checksumType.name() );
            pw.println( "checksum=" + checksum );
        }
    }
    
    
    /**
     * Reads checksum metadata from the file stored alongside the blob data.
     * @return a Map with "checksumType" and "checksum" keys, or null if metadata file doesn't exist
     */
    private Map< String, String > readChecksumMetadata(
            final String bucketName,
            final String objectName,
            final long offset )
    {
        final File metadataFile = getTapeFile( bucketName, objectName + "." + offset + ".checksum" );
        if ( !metadataFile.exists() )
        {
            return null;
        }
        
        final Map< String, String > metadata = new HashMap<>();
        try ( final BufferedReader reader = new BufferedReader( new FileReader( metadataFile ) ) )
        {
            String line;
            while ( ( line = reader.readLine() ) != null )
            {
                final int equalsIndex = line.indexOf( '=' );
                if ( equalsIndex > 0 )
                {
                    final String key = line.substring( 0, equalsIndex );
                    final String value = line.substring( equalsIndex + 1 );
                    metadata.put( key, value );
                }
            }
        }
        catch ( final IOException ex )
        {
            LOG.error( "Failed to read checksum metadata file: " + metadataFile, ex );
            return null;
        }
        
        return metadata;
    }
    
    
    /**
     * Validates the checksum of a blob on tape against the expected checksum.
     * This performs three validations:
     * 1. Checks if stored checksum metadata exists
     * 2. Compares stored checksum type and value with expected values from the blob
     * 3. Recalculates checksum from the actual file data and compares with stored checksum
     * 
     * @return BlobIoFailure if validation fails, null if validation passes
     */
    private BlobIoFailure validateBlobChecksum(
            final String bucketName,
            final String objectName,
            final BlobOnMedia blob )
    {
        // Read stored checksum metadata
        final Map< String, String > storedMetadata = readChecksumMetadata( bucketName, objectName, blob.getOffset() );
        
        if ( storedMetadata == null )
        {
            // No checksum metadata stored - this is acceptable for older data
            LOG.info( "No checksum metadata found for blob " + blob.getId() + ", skipping checksum validation" );
            return null;
        }
        
        final String storedChecksumTypeStr = storedMetadata.get( "checksumType" );
        final String storedChecksum = storedMetadata.get( "checksum" );
        
        if ( storedChecksumTypeStr == null || storedChecksum == null )
        {
            LOG.warn( "Checksum metadata incomplete for blob " + blob.getId() );
            return BeanFactory.newBean( BlobIoFailure.class )
                    .setBlobId( blob.getId() )
                    .setFailure( BlobIoFailureType.CHECKSUM_MISSING );
        }
        
        // Parse stored checksum type
        final ChecksumType storedChecksumType;
        try
        {
            storedChecksumType = ChecksumType.valueOf( storedChecksumTypeStr );
        }
        catch ( final IllegalArgumentException ex )
        {
            LOG.warn( "Unknown checksum algorithm in stored metadata: " + storedChecksumTypeStr );
            return BeanFactory.newBean( BlobIoFailure.class )
                    .setBlobId( blob.getId() )
                    .setFailure( BlobIoFailureType.CHECKSUM_ALGORITHM_UNKNOWN );
        }
        
        // If the blob has expected checksum info, validate against stored metadata
        if ( blob.getChecksumType() != null && blob.getChecksum() != null )
        {
            // Check if checksum algorithm matches
            if ( blob.getChecksumType() != storedChecksumType )
            {
                LOG.warn( "Checksum algorithm mismatch for blob " + blob.getId() 
                        + ": expected " + blob.getChecksumType() + ", stored " + storedChecksumType );
                return BeanFactory.newBean( BlobIoFailure.class )
                        .setBlobId( blob.getId() )
                        .setFailure( BlobIoFailureType.CHECKSUM_ALGORITHM_MISMATCH );
            }
            
            // Check if checksum value matches
            if ( !blob.getChecksum().equals( storedChecksum ) )
            {
                LOG.warn( "Checksum value mismatch for blob " + blob.getId() 
                        + ": expected " + blob.getChecksum() + ", stored " + storedChecksum );
                return BeanFactory.newBean( BlobIoFailure.class )
                        .setBlobId( blob.getId() )
                        .setFailure( BlobIoFailureType.CHECKSUM_VALUE_MISMATCH );
            }
        }
        

        
        LOG.info( "Checksum validation passed for blob " + blob.getId() );
        return null;
    }
    
    
    /**
     * Calculates the checksum of a file using the specified algorithm.
     */
    private String calculateFileChecksum( final File file, final ChecksumType checksumType ) throws IOException
    {
        final byte[] buffer = new byte[ 8192 ];
        int bytesRead;
        
        try ( final InputStream in = new FileInputStream( file ) )
        {
            // Initialize checksum calculator based on type
            final Object checksumCalculator;
            if ( checksumType == ChecksumType.CRC_32 || checksumType == ChecksumType.CRC_32C )
            {
                checksumCalculator = new CRC32();
            }
            else
            {
                try
                {
                    final String algorithm = checksumType == ChecksumType.MD5 ? "MD5" : 
                            checksumType == ChecksumType.SHA_256 ? "SHA-256" : "SHA-512";
                    checksumCalculator = MessageDigest.getInstance( algorithm );
                }
                catch ( final NoSuchAlgorithmException ex )
                {
                    throw new RuntimeException( "Unsupported checksum algorithm: " + checksumType, ex );
                }
            }
            
            // Read and update checksum
            while ( ( bytesRead = in.read( buffer ) ) != -1 )
            {
                if ( checksumCalculator instanceof CRC32 )
                {
                    ( (CRC32) checksumCalculator ).update( buffer, 0, bytesRead );
                }
                else
                {
                    ( (MessageDigest) checksumCalculator ).update( buffer, 0, bytesRead );
                }
            }
            
            // Get the checksum bytes and encode to Base64
            final byte[] checksumBytes;
            if ( checksumCalculator instanceof CRC32 )
            {
                final long crcValue = ( (CRC32) checksumCalculator ).getValue();
                checksumBytes = new byte[] {
                        (byte) ( crcValue >> 24 ),
                        (byte) ( crcValue >> 16 ),
                        (byte) ( crcValue >> 8 ),
                        (byte) crcValue
                };
            }
            else
            {
                checksumBytes = ( (MessageDigest) checksumCalculator ).digest();
            }
            
            return Base64.getEncoder().encodeToString( checksumBytes );
        }
    }

    
    private void validateCanPerformIo()
    {
        if ( null != getDrive().getErrorMessage() )
        {
            throw new IllegalStateException( "Drive is in error: " + getDrive().getErrorMessage() );
        }
        verifyTapeQuiesced();
        verifyTapeNotPreparedForRemoval();
        verifyTapeUsableToUs();
    }
    
    
    public RpcFuture< LtfsFileNamingMode > getLtfsFileNamingMode()
    {
        return new RpcResponse<>( LtfsFileNamingMode.OBJECT_ID );
    }
    
    
    public RpcFuture< String > takeOwnershipOfTape( final UUID tapeId )
    {
        final String checkpoint = UUID.randomUUID().toString();
        writeProperty( ".tapeid", tapeId.toString() );
        writeProperty( ".checkpoint", checkpoint );
        return new RpcResponse<>( checkpoint );
    }
    
    
    public RpcFuture< ? > waitForDriveCleaningToComplete()
    {
        return null;
    }


    public RpcFuture< DriveTestResult > driveTestPostB()
    {
        return null;
    }

    @Override
    public RpcFuture<?> driveDump() {
        return null;
    }


    private String readProperty( final String propertyKey )
    {
        verifyTapeNotPreparedForRemoval();
        m_stateManager.simulateDelay( m_readPropertyDelay );
        synchronized ( m_stateManager )
        {
            final String serialNumber = getDrive().getTapeSerialNumber();
            if ( null == serialNumber )
            {
                throw new IllegalStateException( "No tape in drive." );
            }
            
            final File file = getTapeFile( "", propertyKey );
            if ( file.exists() )
            {
                try
                {
                    final FileInputStream is = new FileInputStream( file );
                    final String value = IOUtils.toString( is, Charset.defaultCharset() );
                    is.close();
                    return value;
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
            }
            return null;
        }
    }

    
    private void writeProperty( final String propertyKey, final String value )
    {
        verifyTapeNotPreparedForRemoval();
        m_stateManager.simulateDelay( m_writePropertyDelay );
        
        synchronized ( m_stateManager )
        {
            final String serialNumber = getDrive().getTapeSerialNumber();
            if ( null == serialNumber )
            {
                throw new IllegalStateException( "No tape in drive." );
            }

            final File file = getTapeFile( "", propertyKey );
            if ( null == value )
            {
                if ( file.exists() )
                {
                    file.delete();
                }
            }
            else
            {
                try
                {
                    final PrintWriter pw = new PrintWriter( file );
                    pw.write( value );
                    pw.close();
                }
                catch ( final FileNotFoundException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
        
        m_stateManager.simulateDelay( m_writePropertyDelay );
    }
    
    
    public RpcFuture< String > quiesce()
    {
        verifyTapeNotPreparedForRemoval();
        verifyTapeUsableToUs();
        getTape().setQuiesceRequired( false );
        final String checkpoint = readProperty( ".checkpoint" );
        return new RpcResponse<>( checkpoint );
    }   
    
    
    public RpcFuture< String > verifyQuiescedToCheckpoint( final String checkpointIdentifier, final boolean allowRollback )
    {
        return new RpcResponse<>( null );
    }
    

    public RpcFuture< String > verifyConsistent()
    {
        return new RpcResponse<>( null );
    }
    
    
    public RpcFuture< Boolean > hasChangedSinceCheckpoint( final String checkpointIdentifier )
    {
        return new RpcResponse<>( Boolean.TRUE );
    }

    
    public RpcFuture< ? > format( boolean characterize, final TapeDriveType tapeDriveTargetedForCompatibility )
    {
        verifyTapeNotPreparedForRemoval();
        m_stateManager.simulateDelay( m_stateManager.getSimulatorConfig().getFormatDelay(), "format tape " + getTape().getBarCode() );
        final SimTape tape = getTape();
        final String tapeId = readProperty( ".tapeid" );
        tape.setContainsForeignData( false );
        try
        {
            FileUtils.deleteDirectory( getTapeFile( "", "formatpending" ).getParentFile() );
            writeProperty(".formatted", "true");
            writeProperty(".loaded", "true");
            if (tapeId != null) {
                writeProperty(".tapeid", tapeId);
            }
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        m_stateManager.simulateDelay(m_stateManager.getSimulatorConfig().getFormatDelay(), "format tape " + tapeId + " (" + tape.getBarCode() + ")");
        writeProperty( ".checkpoint", UUID.randomUUID().toString() );
        if (!tapeIsFormatted()) {
            throw new IllegalStateException("Tape was not formatted.");
        }
        return null;
    }

    
    public RpcFuture< String > inspect()
    {
        verifyTapeNotPreparedForRemoval();
        verifyTapeQuiesced();
        m_stateManager.simulateDelay(m_stateManager.getSimulatorConfig().getInspectDelay(), "inspect tape " + getTape().getBarCode());
        final SimTape tape = getTape();
        if ( tapeIsFormatted() && tape.isContainsForeignData() )
        {
            return new RpcResponse<>( "Tape has data on it." );
        }
        return null;
    }
    
    
    private void verifyTapeQuiesced()
    {
        if ( getTape().isQuiesceRequired() )
        {
            throw new IllegalStateException( 
                    "It's bad practice to proceed before first quiescing the tape drive." );
        }
    }
    
    
    private void verifyTapeNotPreparedForRemoval()
    {
        if ( getTape().isRemovalPrepared() )
        {
            throw new IllegalStateException( "Tape is prepared for removal." );
        }
    }
    
    
    private void verifyTapeUsableToUs()
    {
        if ( !tapeIsFormatted() )
        {
            throw new IllegalStateException( "Tape isn't formatted." );
        }
        if ( getTape().isContainsForeignData() )
        {
            throw new IllegalStateException( "Tape contains foreign data." );
        }
    }
    
    
    private SimDrive getDrive()
    {
        return m_stateManager.getDrive( m_driveSerialNumber );
    }
    
    
    private SimTape getTape()
    {
        final SimDrive drive = getDrive();
        if ( null == drive.getTapeSerialNumber() )
        {
            return null;
        }
        return m_stateManager.getTape( drive.getTapeSerialNumber() );
    }
    
    
    private File getTapeFile( final String bucketName, String filename )
    {
        final SimTape tape = getTape();
        if ( null == tape )
        {
            throw new IllegalStateException( "No tape in drive." );
        }
        
        filename = bucketName + "_" + filename.replace( "/", "_" ).replace( "\\", "_" );
        
        final File dir = new File( m_rootFilesystem + tape.getHardwareSerialNumber() + Platform.FILE_SEPARATOR );
        dir.mkdirs();
        return new File( m_rootFilesystem + tape.getHardwareSerialNumber() + Platform.FILE_SEPARATOR + filename );
    }


    private int m_ioDelay = 0;
    private int m_readPropertyDelay = 0;
    private int m_writePropertyDelay = 0; //called twice per write property
    //NOTE: if this value is non-null we will just sleep instead of writing to the virtual tape
    private Long m_maxBytesPerSecond = null;

    private final SimStateManager m_stateManager;
    
    private final String m_driveSerialNumber;
    private final String m_rootFilesystem;
    
    private final static Logger LOG = Logger.getLogger( SimTapeDriveResource.class );
}
