/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetBucketsRequestHandler extends BaseGetBeansRequestHandler< Bucket >
{
    public GetBucketsRequestHandler()
    {
        super( Bucket.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.BUCKET );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                Bucket.DATA_POLICY_ID,
                UserIdObservable.USER_ID );
    }
    
    
    @Override
    protected WhereClause getCustomFilter( final Bucket requestBean, final CommandExecutionParams params )
    {
        return Require.beanPropertyEqualsOneOf(
                Identifiable.ID, 
                BucketAuthorization.getBucketsUserHasAccessTo( 
                        SystemBucketAccess.STANDARD,
                        BucketAclPermission.LIST, 
                        AdministratorOverride.YES, 
                        params ) );
    }


    @Override
    protected List< Bucket > performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            final List< Bucket > buckets )
    {
        final Map< UUID, Bucket > bucketsMap = BeanUtils.toMap( buckets );
        final Map< UUID, Bucket > nonEmptyBuckets = BeanUtils.toMap( 
                params.getServiceManager().getRetriever( Bucket.class ).retrieveAll( Require.all( 
                        Require.beanPropertyEqualsOneOf( Identifiable.ID, bucketsMap.keySet() ),
                        Require.exists( S3Object.class, S3Object.BUCKET_ID, Require.nothing() ) ) ).toSet() );
        final UUID [] bucketIds = CollectionFactory.toArray( UUID.class, bucketsMap.keySet() );
        final long [] logicalCapacities = params.getPlannerResource().getLogicalUsedCapacity( 
                bucketIds )
                .get( Timeout.DEFAULT ).getCapacities();
        int i = -1;
        for ( final UUID bucketId : bucketIds )
        {
            bucketsMap.get( bucketId ).setEmpty( Boolean.valueOf( null == nonEmptyBuckets.get( bucketId ) ) );
            bucketsMap.get( bucketId ).setLogicalUsedCapacity( Long.valueOf( logicalCapacities[ ++i ] ) );
        }
        
        return buckets;
    }
}
