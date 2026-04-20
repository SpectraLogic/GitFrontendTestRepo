/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.CreateJobIfNecessary;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.RequestCompletedListener;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.DataServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.io.File;
import java.util.UUID;

public final class GetObjectRequestHandler 
    extends BaseRequestHandler
    implements RequestCompletedListener< String >
{
    public GetObjectRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.STANDARD,
                BucketAclPermission.READ,
                AdministratorOverride.NO ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.GET,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.REQUIRED ) );
        
        registerOptionalRequestParameters(
                RequestParameterType.JOB,
                RequestParameterType.OFFSET,
                RequestParameterType.CACHED_ONLY,
                RequestParameterType.VERSION_ID );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        m_commandExecutionParams = params;

        final boolean cachedOnly = params.getRequest().hasRequestParameter( RequestParameterType.CACHED_ONLY );
        final boolean hasJobId = params.getRequest().hasRequestParameter( RequestParameterType.JOB );
        if ( cachedOnly && hasJobId )
        {
            throw new S3RestException(AWSFailure.INVALID_ARGUMENT,
                    "\"Cached Only\" request is not legal for existing job." +
                            " Please specify either job ID or cachedOnly flag, not both.");
        }

        final UUID jobId;
        final UUID blobId;
        
        final CreateJobIfNecessary createJobIfNecessary = new CreateJobIfNecessary( JobRequestType.GET );
        jobId = createJobIfNecessary.execute( params );
        blobId = createJobIfNecessary.getBlobId();

        String cacheFilename = null;
        String fileSource = null;
        final Blob blob = params.getServiceManager().getRetriever( Blob.class ).attain( blobId );
        if ( 0 == blob.getLength() )
        {
            verifyObjectCreated( params, blob.getObjectId() );
        }
        else
        {
            final RpcFuture<DiskFileInfo> jmf = params.getPlannerResource().startBlobRead( jobId, blobId );
            try
            {
                final DiskFileInfo diskFileInfo = jmf.get(Timeout.LONG);
                cacheFilename = diskFileInfo.getFilePath();
                fileSource = DiskFileInfo.source(diskFileInfo);

            }
            catch ( final RuntimeException ex )
            {
                if (cachedOnly) {
                    //NOTE: We should not hit this, it should have been hit already in CreateJobIfNecessary
                    throw new S3RestException(
                            AWSFailure.SLOW_DOWN_BY_CLIENT,
                            "The object is not currently available in cache." );
                }
                verifyObjectCreated( params, blob.getObjectId() );
                throw new S3RestException(
                        AWSFailure.SLOW_DOWN_BY_SERVER,
                        "The object is not in cache yet.", 
                        ex ).setRetryAfter( 30 );
            }
            if ( null == cacheFilename )
            {
                throw new RuntimeException( "Cache filename cannot be null." );
            }
            final File cacheFile = new File( cacheFilename );
            if ( !cacheFile.exists() ) {
                LOG.error("No cache file found at: " + cacheFilename + " .");
                throw new S3RestException(
                        GenericFailure.INTERNAL_ERROR,
                        "File expected in cache but not found for blob: " + blobId );
            } else if ( cacheFile.length() != blob.getLength()) {
                throw new S3RestException(
                        GenericFailure.INTERNAL_ERROR,
                        "Blob should have length " + blob.getLength()+ " but cache file length is "
                                + cacheFile.length() + " for blob: " + blobId );
            }
        }

        params.getServiceManager().getService( S3ObjectPropertyService.class ).populateAllHttpHeaders(
                blobId, request.getHttpResponse() );
        
        String responseType = request.getHttpRequest().getContentType();
        if ( responseType != null && responseType.contains( "/xml" ) )
        {
            responseType = "xml";
        }

        return DataServlet.serviceRequest(
                params, 
                cacheFilename,
                fileSource,
                jobId,
                blobId,
                blob.getByteOffset(),
                createJobIfNecessary.getByteRanges(),
                blob.getChecksumType(),
                blob.getChecksum(),
                this,
                responseType );
    }
    
    
    private void verifyObjectCreated( final CommandExecutionParams params, final UUID objectId )
    {
        final S3Object o = 
                params.getServiceManager().getRetriever( S3Object.class ).attain( objectId );
        if ( null == o.getCreationDate() )
        {
            throw new S3RestException(
                    AWSFailure.SLOW_DOWN_BY_CLIENT,
                    "The object has not been uploaded yet." ).setRetryAfter( 30 );
        }
    }
    

    @Override
    public void requestCompleted( final String jobIdObjectIdAsString )
    {
        final UUID jobId;
        final UUID blobId;
        final String [] elements = jobIdObjectIdAsString.split( "," );
        if ( 2 == elements.length )
        {
            jobId = UUID.fromString( elements[ 0 ] );
            blobId = UUID.fromString( elements[ 1 ] );
        }
        else
        {
            jobId = null;
            blobId = UUID.fromString( elements[ 0 ] );
        }
        
        if ( null == blobId )
        {
            LOG.info( "Ignoring request completed since no blob id was provided." );
            return;
        }

        m_commandExecutionParams.getPlannerResource().blobReadCompleted( jobId, blobId );
    }
}
