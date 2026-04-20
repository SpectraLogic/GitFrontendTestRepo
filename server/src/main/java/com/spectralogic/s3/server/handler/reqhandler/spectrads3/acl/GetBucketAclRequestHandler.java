/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetBucketAclRequestHandler extends BaseGetBeanRequestHandler< BucketAcl >
{
    public GetBucketAclRequestHandler()
    {
        super( BucketAcl.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.BUCKET_ACL );
    }

    
    @Override
    protected BucketAcl performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            final BucketAcl acl )
    {
        BucketAuthorization.verify( 
                SystemBucketAccess.STANDARD,
                BucketAclPermission.OWNER, 
                AdministratorOverride.YES, 
                params,
                params.getServiceManager().getRetriever( Bucket.class ).attain( acl.getBucketId() ) );
        return acl;
    }
}
