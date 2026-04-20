/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.orm.S3ObjectRM;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.marshal.DateMarshaler;
import com.spectralogic.util.security.ChecksumType;

public final class HeadObjectRequestHandler extends BaseRequestHandler
{
    public HeadObjectRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.STANDARD,
                BucketAclPermission.LIST,
                AdministratorOverride.NO ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.HEAD,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.REQUIRED ) );
        registerOptionalRequestParameters(
                RequestParameterType.VERSION_ID );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansServiceManager serviceManager = params.getServiceManager();
        final UUID objectId = serviceManager.getService( S3ObjectService.class )
                .attainId( request.getBucketName(), request.getObjectName(), getVersion( params ), false );
        final S3Object object = new S3ObjectRM(objectId, serviceManager).unwrap();
        final HttpServletResponse response = request.getHttpResponse();
        serviceManager.getService( S3ObjectPropertyService.class )
                      .populateObjectHttpHeaders( objectId, response );
    
        List< Blob > blobs = serviceManager.getRetriever( Blob.class )
                                           .retrieveAll( Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ) )
                                           .toList();
    
        final ChecksumType checksumType = blobs.get( 0 )
                                               .getChecksumType();
        final Date date = object.getCreationDate();
        if (date != null)
        {
        	response.addHeader( "creation-date", DateMarshaler.marshal(date) );
        }
        response.addHeader( "version-id", objectId.toString() );
        if ( null != checksumType )
        {
            response.addHeader( "ds3-blob-checksum-type", checksumType.toString() );
            blobs.stream()
                 .forEach( x -> {
                     final Blob blob = serviceManager.getRetriever( Blob.class )
                                                     .attain( x.getId() );
                     if ( null != blob.getChecksumType() )
                     {
                         final String checksumHeader =
                                 String.format( "ds3-blob-checksum-offset-%d", blob.getByteOffset() );
                         response.addHeader( checksumHeader, blob.getChecksum() );
                     }
                 } );
        }
    
        response.addHeader( S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(),
                String.valueOf( serviceManager.getService( S3ObjectService.class ).getSizeInBytes( 
                        CollectionFactory.toSet( objectId ) ) ) );
        
        return BeanServlet.serviceGet( params, null );
    }
    
    private UUID getVersion( final CommandExecutionParams params )
    {
        if ( null != params.getRequest().getRequestParameter( RequestParameterType.VERSION_ID ) )
        {
            return params.getRequest().getRequestParameter( RequestParameterType.VERSION_ID ).getUuid();
        }
        return null;
    }
}
