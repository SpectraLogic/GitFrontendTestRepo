/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetDegradedBucketsRequestHandler extends BaseGetBeansRequestHandler< Bucket >
{
    public GetDegradedBucketsRequestHandler()
    {
        super( Bucket.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
                RestDomainType.DEGRADED_BUCKET );
         
        registerOptionalBeanProperties(
                NameObservable.NAME,
                Bucket.DATA_POLICY_ID,
                UserIdObservable.USER_ID );
     }
     
     
     @Override
     protected WhereClause getCustomFilter( final Bucket requestBean, final CommandExecutionParams params )
     {
         return Require.all( 
                 Require.exists( DegradedBlob.class, DegradedBlob.BUCKET_ID, Require.nothing() ),
                 Require.beanPropertyEqualsOneOf(
                         Identifiable.ID, 
                         BucketAuthorization.getBucketsUserHasAccessTo( 
                                 SystemBucketAccess.STANDARD,
                                 BucketAclPermission.LIST, 
                                 AdministratorOverride.YES, 
                                 params ) ) );
     }
}
