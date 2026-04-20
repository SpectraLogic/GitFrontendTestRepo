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
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteBucketAclRequestHandler extends BaseDeleteBeanRequestHandler< BucketAcl >
{
    public DeleteBucketAclRequestHandler()
    {
        super( BucketAcl.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.BUCKET_ACL );
    }
    
    
    @Override
    protected void deleteBean( final CommandExecutionParams params, final BucketAcl acl )
    {
        if ( null == acl.getBucketId() )
        {
            new DefaultPublicExposureAuthenticationStrategy(
                    RequiredAuthentication.ADMINISTRATOR ).authenticate( params );
        }
        else
        {
            BucketAuthorization.verify(
                    SystemBucketAccess.STANDARD, 
                    BucketAclPermission.OWNER,
                    AdministratorOverride.YES, 
                    params,
                    params.getServiceManager().getRetriever( Bucket.class ).attain( acl.getBucketId() ) );
        }
        super.deleteBean( params, acl );
    }
}
