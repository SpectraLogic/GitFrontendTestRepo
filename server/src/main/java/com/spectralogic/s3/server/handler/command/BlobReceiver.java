/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.command;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.s3.server.domain.ReceivedBlob;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.frmwrk.IoInProgressNotifier;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.SingleInputStreamProvider;
import com.spectralogic.util.io.ThreadedDataMover;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.security.ChecksumType;
import static java.nio.file.StandardOpenOption.*;

public class BlobReceiver extends BaseCommand< ReceivedBlob >
{
    public BlobReceiver(
            final String filename,
            final Job job,
            final UUID blobId,
            final DataPolicy dataPolicy,
            long declaredContentLength )
    {
        m_filename = filename;
        m_jobId = job.getId();
        m_replicating = job.isReplicating();
        m_blobId = blobId;
        m_dataPolicy = dataPolicy;
        m_declaredContentLength = declaredContentLength;
        Validations.verifyNotNull( "filename", m_filename );
        Validations.verifyNotNull( "jobId", m_jobId );
        Validations.verifyNotNull( "blobId", m_blobId );
        Validations.verifyNotNull( "bucket", m_dataPolicy );
    }


    @Override
    protected ReceivedBlob executeInternal( final CommandExecutionParams params )
    {
        final CacheFilesystem cacheFilesystem = params.getServiceManager()
                                                      .getRetriever( CacheFilesystem.class )
                                                      .retrieveAll()
                                                      .getFirst();
        Validations.verifyNotNull( "cacheFilesystem", cacheFilesystem );
        
        final IoInProgressNotifier activityLogger = new IoInProgressNotifier(
                m_jobId,
                m_blobId, 
                params.getPlannerResource(),
                params.getServiceManager().getRetriever( Blob.class ) );
        final DS3Request request = params.getRequest();
        final ChecksumType checksumType = determineChecksumType( request );
        OutputStream os = null;
        FileChannel channel;
        try
        {
            Set< OpenOption > options = new HashSet<>();
            options.add( WRITE );
            if ( cacheFilesystem.getCacheSafetyEnabled() )
            {
                options.add( SYNC );
            }
            options.add( CREATE );
    
            channel = FileChannel.open( Paths.get(m_filename), options);
            // This output stream does no buffering of data, which is why we like it.
            os = Channels.newOutputStream( channel );
            
            final ThreadedDataMover dataMover = new ThreadedDataMover(
                    HardwareInformationProvider.getTomcatBufferSize(),
                    HardwareInformationProvider.getZfsCacheFilesystemRecordSize(),
                    "Receive object from client", 
                    m_declaredContentLength,
                    checksumType,
                    os, 
                    new SingleInputStreamProvider( request.getHttpRequest().getInputStream() ), 
                    activityLogger );
            dataMover.run();
            channel.force( false );
            activityLogger.ensureBlobStillExists();
            
            final ReceivedBlob retval = BeanFactory.newBean( ReceivedBlob.class );
            retval.setSizeRead( dataMover.getTotalBytesMoved() );
            retval.setFileName( m_filename );
            retval.setChecksumType( checksumType );
            retval.setChecksum( Base64.encodeBase64String( dataMover.getChecksum() ) );
            verifyPutObjectRequest(
                    retval, 
                    request.getHttpRequest().getHeader( checksumType ), 
                    m_declaredContentLength );
            return retval;
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to receive blob from client.", ex);
        }
        finally
        {
            try
            {
                if ( null != os )
                {
                    // Closing the stream also closes the channel it was derived from
                    os.close();
                }
                request.getHttpRequest().getInputStream().close();
            }
            catch ( final IOException ex )
            {
                LOG.error( "Failed to close output file stream for " + os + ".", ex );
            }
        }
    }
    
    
    private void verifyPutObjectRequest(
            final ReceivedBlob receivedBlob,
            final String checksumFromClient,
            final long contentLengthHeaderValue )
    {
        if ( receivedBlob.getSizeRead() != contentLengthHeaderValue )
        {
            throw new S3RestException(
                    AWSFailure.INCOMPLETE_BODY,
                    "Read " + receivedBlob.getSizeRead() 
                    + " bytes, but client-declared content length in the HTTP request was "
                    + contentLengthHeaderValue + " bytes." );
        }
        
        if ( checksumFromClient != null && !checksumFromClient.equals( receivedBlob.getChecksum() ) )
        {
            throw new S3RestException(
                    AWSFailure.BAD_DIGEST,
                    "Validation Failure, checksum mismatch.  Client declared " 
                    + receivedBlob.getChecksumType() + " to be '"
                    + checksumFromClient + "', but it was '" + receivedBlob.getChecksum() + "'" );
        }
    }
    
    
    private ChecksumType determineChecksumType( final DS3Request request )
    {
        final ChecksumType clientChecksumType = ChecksumType.fromHttpRequest( request.getHttpRequest() );
        if ( null == clientChecksumType && m_dataPolicy.isEndToEndCrcRequired() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST,
                    "End-to-end CRC is required for this bucket.  " 
                    + "You are required to calculate and transmit a "
                    + m_dataPolicy.getChecksumType().getAlgorithmName()
                    + " to ensure data integrity." );
        }
        if ( null == clientChecksumType && m_replicating )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST,
                    "End-to-end CRC is required for replicating jobs." );
        }
        if ( null != clientChecksumType && clientChecksumType != m_dataPolicy.getChecksumType() )
        {
            throw new S3RestException( 
                    GenericFailure.BAD_REQUEST,
                    "CRCs for this bucket must be of type " 
                    + m_dataPolicy.getChecksumType().getAlgorithmName() + ".  You transmitted a " 
                    + clientChecksumType.getAlgorithmName() + "." );
        }
        return m_dataPolicy.getChecksumType();
    }

    
    private final String m_filename;
    private final UUID m_jobId;
    private final UUID m_blobId;
    private final DataPolicy m_dataPolicy;
    private final boolean m_replicating;
    private final long m_declaredContentLength;
}
