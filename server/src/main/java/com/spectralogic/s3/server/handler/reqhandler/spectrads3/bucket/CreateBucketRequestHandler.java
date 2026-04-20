/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.SystemBucketProtectedAuthorizationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.DataPolicyUtil;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateBucketRequestHandler extends BaseCreateBeanRequestHandler< Bucket >
{
    public CreateBucketRequestHandler()
    {
        super( Bucket.class, 
               new SystemBucketProtectedAuthorizationStrategy(),
               RestDomainType.BUCKET,
               DefaultUserIdToUserMakingRequest.YES );
        
        registerOptionalBeanProperties(
                Bucket.DATA_POLICY_ID,
                Identifiable.ID,
                Bucket.PROTECTED );
        registerBeanProperties( 
                UserIdObservable.USER_ID,
                Bucket.NAME );
    }

    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final Bucket bucket )
    {
        final DataPolicy dataPolicy = params.getServiceManager().getRetriever( DataPolicy.class ).attain( 
                DataPolicyUtil.getDataPolicy( params, bucket.getDataPolicyId() ) );
    
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
    
        bucket.setDataPolicyId( dataPolicy.getId() );
        if ( null == bucket.getId() )
        {
            bucket.setId( UUID.randomUUID() );
        }
        return params.getTargetResource().createBucket( bucket ).get( Timeout.LONG );
    }
}
