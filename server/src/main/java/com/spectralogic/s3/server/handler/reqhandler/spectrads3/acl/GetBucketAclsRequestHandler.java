/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetBucketAclsRequestHandler extends BaseGetBeansRequestHandler< BucketAcl >
{
    public GetBucketAclsRequestHandler()
    {
        super( BucketAcl.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.BUCKET_ACL );
         
        registerOptionalBeanProperties(
                BucketAcl.BUCKET_ID,
                UserIdObservable.USER_ID,
                BucketAcl.GROUP_ID, 
                BucketAcl.PERMISSION );
    }

    
    @Override
    protected WhereClause getCustomFilter( final BucketAcl requestBean, final CommandExecutionParams params )
    {
        if ( null == params.getRequest().getAuthorization().getUser() 
                || params.getRequest().getAuthorization().getUser().getId().equals( 
                        requestBean.getUserId() ) )
        {
            return null;
        }
        if ( null == requestBean.getBucketId() )
        {
            new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR )
                .authenticate( params );
            return null;
        }
        
        return Require.beanPropertyEqualsOneOf( 
                BucketAcl.BUCKET_ID, 
                BucketAuthorization.getBucketsUserHasAccessTo(
                        SystemBucketAccess.STANDARD,
                        BucketAclPermission.OWNER,
                        AdministratorOverride.YES,
                        params ) );
    }
}
