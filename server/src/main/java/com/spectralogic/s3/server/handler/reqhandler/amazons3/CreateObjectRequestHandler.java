/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.aws.S3Utils;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.ReceivedBlob;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.BlobReceiver;
import com.spectralogic.s3.server.handler.command.CreateJobIfNecessary;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateObjectRequestHandler extends BaseRequestHandler
{
    public CreateObjectRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.WRITE,
                AdministratorOverride.NO ), 
               new NonRestfulCanHandleRequestDeterminer(
                RequestType.PUT,
                BucketRequirement.REQUIRED, 
                S3ObjectRequirement.REQUIRED ) );
        
        requiresContentLengthHttpHeader();
        registerOptionalRequestParameters(
                RequestParameterType.JOB,
                RequestParameterType.OFFSET );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final String bucketName = request.getBucketName();
        final String objectName = request.getObjectName();
        final Bucket bucket = params.getServiceManager().getRetriever( Bucket.class ).attain( 
                Bucket.NAME, bucketName );
        final DataPolicy dataPolicy = params.getServiceManager().getRetriever( DataPolicy.class ).attain( 
                bucket.getDataPolicyId() );
        
        boolean error = true;
        boolean keepFile = false;

        final List< S3ObjectProperty > objectProperties;
        ReceivedBlob receivedBlob = null;
        try 
        {
            LOG.info( "Will create '" + objectName + "' in bucket '" + bucketName + "'." );
            final CreateJobIfNecessary createJobIfNecessary =
                    new CreateJobIfNecessary( JobRequestType.PUT );
            final UUID jobId = createJobIfNecessary.execute( params );
            final Job job = params.getServiceManager().getRetriever( Job.class ).attain( jobId );
            final UUID blobId = createJobIfNecessary.getBlobId();
            final Blob blob = params.getServiceManager().getRetriever( Blob.class ).attain( blobId );
            final UUID objectId = blob.getObjectId();
            synchronized ( s_blobWriteLocks )
            {
                if ( s_blobWriteLocks.contains( blobId ) )
                {
                    throw new S3RestException( 
                            GenericFailure.CONFLICT, 
                            "Blob " + blobId + " for object " + objectId + " is already being uploaded." );
                }
                else
                {
                    s_blobWriteLocks.add( blobId );
                }
            }
            try
            {
                objectProperties = S3Utils.buildObjectPropertiesFromAmzCustomMetadataHeaders( 
                        request.getHttpRequest() );
                if ( null != blob.getChecksumType() )
                {
                    switch ( dataPolicy.getVersioning() )
                    {
                        case NONE:
                            throw new S3RestException( 
                                    GenericFailure.CONFLICT, 
                                    "Blob " + blobId + " for object " + objectId + " has already been put." );
                        case KEEP_LATEST:
                            throw new S3RestException( 
                                    GenericFailure.CONFLICT, 
                                    "Blob " + blobId + " for object " + objectId + " has already been put."
                                    + "  If you desire to upload a newer version of this object, "
                                    + "you must either (i) wait for the current version to be entirely persisted"
                                    + ", or (ii) create a PUT job so that it's clear you intend to create"
                                    + " a newer version rather than are attempting to upload the already-existant"
                                    + " object again." );
                        case KEEP_MULTIPLE_VERSIONS:
                            break;
                        default:
                            throw new UnsupportedOperationException( 
                                    "No code to support: " + dataPolicy.getVersioning() );
                    }
                }
    
                final String objectCreationDateHeader = request.getHttpRequest().getHeader( 
                        S3HeaderType.OBJECT_CREATION_DATE.getHttpHeaderName() );
                final Long objectCreationDate = ( null == objectCreationDateHeader ) ?
                        null 
                        : Long.valueOf( Long.parseLong( objectCreationDateHeader ) );

                final String cacheFile = params.getPlannerResource().startBlobWrite(
                        jobId,
                        blobId ).get( Timeout.LONG );
                final long declaredContentLength = Long.parseLong( request.getRequestHeader( 
                        S3HeaderType.CONTENT_LENGTH ) );
                receivedBlob = new BlobReceiver(
                        cacheFile,
                        job,
                        blobId,
                        dataPolicy, 
                        declaredContentLength
                ).execute( params );
            
                keepFile = true;
                final Boolean completionResult = params.getPlannerResource().blobWriteCompleted( 
                        jobId, 
                        blobId, 
                        receivedBlob.getChecksumType(), 
                        receivedBlob.getChecksum(),
                        objectCreationDate,
                        CollectionFactory.toArray( S3ObjectProperty.class, objectProperties ) )
                            .get( Timeout.LONG );
                if ( job.isReplicating() && null != completionResult )
                {
                    if ( !completionResult.booleanValue() )
                    {
                        throw new S3RestException(
                                GenericFailure.BAD_REQUEST, 
                                "Replicating jobs must specify the "
                                + S3HeaderType.OBJECT_CREATION_DATE.getHttpHeaderName() + " HTTP header." );
                    }
                }
                
                params.getServiceManager().getService( S3ObjectPropertyService.class ).populateAllHttpHeaders(
                        blobId, request.getHttpResponse() );
                error = false;
            }
            finally
            {
                synchronized ( s_blobWriteLocks )
                {
                    s_blobWriteLocks.remove( blobId );
                }
            }
        }
        finally
        {
            if ( error && receivedBlob != null && !keepFile )
            {
                final String cacheFile = receivedBlob.getFileName();

                if ( cacheFile != null )
                {
                    final File f = new File( cacheFile );

                    if ( f.exists() )
                    {
                        if ( !f.delete() )
                        {
                            LOG.warn( "Cache file could not be cleaned up: " + cacheFile );
                        }
                    }
                }
            }
        }

        return BeanServlet.serviceRequest( params, HttpServletResponse.SC_OK, null );
    }
    
	
    private final static Set< UUID > s_blobWriteLocks = new HashSet<>();
}
