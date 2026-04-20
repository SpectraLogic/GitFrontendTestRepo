/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import javax.servlet.http.HttpServletResponse;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.SystemBucketProtectedAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.DataPolicyUtil;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateBucketRequestHandler extends BaseRequestHandler
{
    public CreateBucketRequestHandler()
    {
        super( new SystemBucketProtectedAuthorizationStrategy(), 
               new NonRestfulCanHandleRequestDeterminer(
                RequestType.PUT, 
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        Bucket bucket = BeanFactory.newBean( Bucket.class );
        bucket.setName( request.getBucketName() );
        bucket.setUserId( params.getRequest().getAuthorization().getUser().getId() );
        
        final BucketService bucketService = params.getServiceManager().getService( BucketService.class );
        final Bucket existantBucket = bucketService.retrieve( Bucket.NAME, bucket.getName() );
        if ( null != existantBucket )
        {
            throw new S3RestException( 
                    ( existantBucket.getUserId().equals( bucket.getUserId() ) ) ?
                            AWSFailure.BUCKET_ALREADY_OWNED_BY_YOU 
                            : AWSFailure.BUCKET_ALREADY_EXISTS, 
                    "A bucket with the name " + bucket.getName() + " already exists." );
        }
    
        User user = params.getServiceManager()
                          .getService( UserService.class )
                          .attain( bucket.getUserId() );
        final int currentBuckets = params.getServiceManager()
                                         .getService( BucketService.class )
                                         .getCount( Require.beanPropertyEquals( Bucket.USER_ID, user.getId() ) );
        if ( user.getMaxBuckets() <= currentBuckets )
        {
            throw new S3RestException( GenericFailure.FORBIDDEN,
                    "User " + user.getName() + " has reached the maximum number of buckets they can create at " +
                            user.getMaxBuckets() + "." );
        }
    
        bucket.setDataPolicyId( DataPolicyUtil.getDataPolicy( params, (UUID)null ) );
        bucket.setId( UUID.randomUUID() );
        bucket = bucketService.attain( 
                params.getTargetResource().createBucket( bucket ).get( Timeout.LONG ) );

        return BeanServlet.serviceRequest( params, HttpServletResponse.SC_OK, null );
    }
}
