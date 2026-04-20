package com.spectralogic.s3.server.servlet;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.s3.server.frmwrk.IoInProgressNotifier;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.RequestCompletedListener;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.io.CLibrary;
import com.spectralogic.util.io.SingleInputStreamProvider;
import com.spectralogic.util.io.ThreadedDataMover;
import com.spectralogic.util.io.lang.ByteRanges;
import com.spectralogic.util.security.ChecksumType;

/**
 * Servlet that serves raw data.
 */
public class DataServlet extends BaseServlet< DataServletParams >
{
    private static final Logger LOG = Logger.getLogger(DataServlet.class);
    
    
    public DataServlet()
    {
        super( RequestType.GET );
    }
    
    
    public static ServletResponseStrategy serviceRequest( 
            final CommandExecutionParams commandExecutionParams,
            final String cacheFile,
            final String fileSource,
            final UUID jobId,
            final UUID blobId,
            final long blobOffset,
            final ByteRanges byteRanges,
            final ChecksumType checksumType,
            final String checksum,
            final RequestCompletedListener< String > requestCompletedListener,
            final String responseType )
    {
        final DataServletParams dsParams = BeanFactory.newBean( DataServletParams.class )
                .setCacheFile( cacheFile )
                .setFileSource( fileSource )
                .setJobId( jobId )
                .setBlobId( blobId )
                .setBlobOffset( blobOffset )
                .setByteRanges( byteRanges )
                .setResource( commandExecutionParams.getPlannerResource() )
                .setRequestCompletedListener( requestCompletedListener )
                .setResponseType( responseType )
                .setChecksumType( checksumType )
                .setChecksum( checksum );
        save( commandExecutionParams, dsParams );
        
        return SERVLET_SPEC;
    }
    

    @Override
    public void provideResponse(
            final DataServletParams params,
            final RequestType requestType,
            HttpServletRequest request,
            HttpServletResponse response)
    {
        LOG.info( "Will push '" + params.getCacheFile() 
                   + "' to client in format '" + params.getResponseType() + "'." );

        if (params.getCacheFile() == null)
        {
            LOG.info( "No cache file was provided to send binary data to client." );
            response.addHeader( 
                    S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(),
                    "0" );
            response.setStatus( HttpServletResponse.SC_OK );
            notifyRequestCompletedListener(params);
            return;
        }

        // Set the status before we start writing data because the status is the
        // first part of an HTTP request and setStatus won't do anything if data
        // has already been written.
        if ( null == params.getByteRanges() )
        {
            response.setStatus( HttpServletResponse.SC_OK );
            response.addHeader( 
                    S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(), 
                    String.valueOf( new File( params.getCacheFile() ).length() ) );
        }
        else
        {
            response.setStatus( HttpServletResponse.SC_PARTIAL_CONTENT );
            response.addHeader( 
                    S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(),
                    String.valueOf( params.getByteRanges().getAggregateLength() ) );
            response.addHeader( 
                    S3HeaderType.CONTENT_BYTE_RANGES.getHttpHeaderName(), 
                    params.getByteRanges().toString() );
            response.addHeader( 
                    S3HeaderType.ACCEPT_BYTE_RANGES.getHttpHeaderName(), 
                    "bytes" );
        }

        OutputStream os = null;
        try
        {
            final IoInProgressNotifier activityLogger = new IoInProgressNotifier(
                    params.getJobId(),
                    params.getBlobId(), 
                    params.getResource() );
            if ( null != params.getChecksumType() )
            {
                if ( ! response.containsHeader( params.getChecksumType().getHttpHeaderName() ) )
                {
                    response.addHeader( 
                        params.getChecksumType().getHttpHeaderName(), 
                        params.getChecksum() );
                }
            }
            
            
            
            final ByteRanges blobRangesToTransfer = ( params.getByteRanges() == null) ?
                     null
                     : params.getByteRanges().shift( -params.getBlobOffset() );
            os = response.getOutputStream();
            sendData( 
                    activityLogger,
                    os,
                    params.getCacheFile(),
                    params.getFileSource(),
                    blobRangesToTransfer );
            notifyRequestCompletedListener(params);
        }
        catch (IOException ex)
        {
            LOG.warn( ExceptionUtil.getMessageWithSingleLineStackTrace( 
                    "Error writing data back to client.", ex ) );
            // If we've already written to the OutputStream then this line of code won't do anything.
            response.setStatus( HttpServletResponse.SC_INTERNAL_SERVER_ERROR );
        }
        finally {
            try
            {
                if ( os != null )
                {
                    os.close();
                }
            }
            catch ( final IOException ioe )
            {
                LOG.error( "Error closing request output stream", ioe );
            }
        }
    }
    
    
    private void notifyRequestCompletedListener( final DataServletParams params )
    {
        if ( params.getRequestCompletedListener() != null )
        {
            params.getRequestCompletedListener().requestCompleted(
                    ( ( null == params.getJobId() ) ? "" : params.getJobId().toString() + "," )
                    + params.getBlobId().toString() );
        }
    }
    

    private static void sendData(
            final IoInProgressNotifier activityLogger,
            final OutputStream os, 
            final String fileName,
            final String fileSource,
            final ByteRanges byteRanges ) throws IOException
    {
        final String byteRangeMsg = ( null == byteRanges ) ? "" : " " + byteRanges.toString();
        LOG.info( "Writing " + fileName + byteRangeMsg + "..." );
        
        FileInputStream is = null;
        try
        {
            is = new FileInputStream( fileName );
            CLibrary.discardDataAfterSingleUse( is.getFD() );
            new ThreadedDataMover( 
                    HardwareInformationProvider.getZfsCacheFilesystemRecordSize(),
                    HardwareInformationProvider.getTomcatBufferSize(),
                    "Send object from " + fileSource + " to client",
                    new File( fileName ).length(),
                    byteRanges,
                    null,
                    os, 
                    new SingleInputStreamProvider( is ), 
                    activityLogger ).run();
        }
        finally
        {
            try
            {
                if ( is != null )
                {
                    is.close();
                }
            }
            catch ( final IOException ioe )
            {
                LOG.error( "Error closing/flushing closing cache get file", ioe );
            }
        }
    }


    private final static ServletResponseStrategy SERVLET_SPEC = 
            new ServletResponseStrategyImpl( DataServlet.class );
}
