/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailure;
import com.spectralogic.s3.server.exception.S3RestException;
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
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class DeleteObjectRequestHandler extends BaseRequestHandler
{
    public DeleteObjectRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.DELETE,
                AdministratorOverride.NO ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.DELETE,
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
        final String bucketName = request.getBucketName();
        final String objectName = request.getObjectName();
        
        final S3ObjectService objectService = params.getServiceManager().getService( S3ObjectService.class );
        final BucketService bucketService = params.getServiceManager().getService( BucketService.class );
        final DataPolicy dataPolicy =
        		new BucketRM( bucketService.attain( Bucket.NAME, bucketName ), params.getServiceManager() )
        			.getDataPolicy().unwrap();
                
        final boolean unversionedDeletesAffectIncompleteObjects =
        		VersioningLevel.KEEP_MULTIPLE_VERSIONS != dataPolicy.getVersioning();
        final UUID objectId = objectService.attainId( bucketName, objectName, getVersion( params ),
        		unversionedDeletesAffectIncompleteObjects );
        

        final DeleteObjectFailure[] failures = params.getTargetResource().deleteObjects(
                        request.getAuthorization().getUserId(),
                        PreviousVersions.determineHandling(
                                request.hasRequestParameter( RequestParameterType.VERSION_ID ),
                                dataPolicy.getVersioning() ),
                        new UUID [] { objectId } )
                .get( Timeout.LONG )
                .getFailures();
        switch ( failures.length )
        {
            case 0:
                return BeanServlet.serviceDelete( params, null );
            case 1:
                switch ( failures[0].getReason() )
                {
                    case NOT_FOUND:
                        throw new S3RestException(
                                GenericFailure.NOT_FOUND,
                                "The object named " + objectName + " does not exist." );
                    default:
                        throw new UnsupportedOperationException(
                                "No code to handle delete failure reason: "
                                + failures[0].getReason().toString() );
                }
            default:
                throw new IllegalStateException( "Single object delete returned multiple failures." );
        }
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
