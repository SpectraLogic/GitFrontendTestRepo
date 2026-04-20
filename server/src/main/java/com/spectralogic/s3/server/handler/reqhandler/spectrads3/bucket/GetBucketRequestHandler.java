/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetBucketRequestHandler extends BaseGetBeanRequestHandler< Bucket >
{
    public GetBucketRequestHandler()
    {
        super( Bucket.class,
               new BucketAuthorizationStrategy(
                    SystemBucketAccess.STANDARD, 
                    BucketAclPermission.LIST, 
                    AdministratorOverride.YES ), 
               RestDomainType.BUCKET );
    }

    
    @Override
    protected Bucket performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            final Bucket bucket )
    {
        bucket.setEmpty( Boolean.valueOf( 
                null == params.getServiceManager().getRetriever( Bucket.class ).retrieve( Require.all( 
                        Require.beanPropertyEquals( Identifiable.ID, bucket.getId() ),
                        Require.exists( S3Object.class, S3Object.BUCKET_ID, Require.nothing() ) ) ) ) );
        bucket.setLogicalUsedCapacity( Long.valueOf( params.getPlannerResource().getLogicalUsedCapacity(
                new UUID [] { bucket.getId() } ).get( Timeout.DEFAULT ).getCapacities()[ 0 ] ) );
        return bucket;
    }
}
