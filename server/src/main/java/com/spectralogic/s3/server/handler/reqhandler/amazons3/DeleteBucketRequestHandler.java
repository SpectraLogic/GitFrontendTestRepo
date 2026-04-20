/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class DeleteBucketRequestHandler extends BaseRequestHandler
{
    public DeleteBucketRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.DELETE,
                AdministratorOverride.NO ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.DELETE,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED ) );
    }
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Bucket bucket = params.getServiceManager().getRetriever( Bucket.class )
                .attain( Bucket.NAME, request.getBucketName() );

        if ( bucket.isProtected() )
        {
            throw new S3RestException( GenericFailure.CONFLICT, "The bucket " +
                    bucket.getName() + " cannot be deleted because it is protected by an application. The " +
                    Bucket.PROTECTED + " flag must be removed before this bucket can be deleted." );
        }

        final int protectedJobCount = params.getServiceManager().getRetriever( Job.class ).getCount(
                Require.all(
                        Require.beanPropertyEquals( Job.BUCKET_ID, bucket.getId() ),
                        Require.beanPropertyEquals( Job.PROTECTED, true ) ) );

        if ( protectedJobCount > 0 )
        {
            throw new S3RestException(
                    GenericFailure.CONFLICT,
                    "The bucket " + bucket.getName() + " cannot be deleted because it has " + protectedJobCount + " protected active jobs.");
        }
        
        params.getTargetResource().deleteBucket( 
                request.getAuthorization().getUserId(),
                bucket.getId(),
                false ).get( Timeout.LONG );
        
        return BeanServlet.serviceDelete( params, null );
    }
}
