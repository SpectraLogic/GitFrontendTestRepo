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
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateBucketAclForUserRequestHandler extends BaseCreateBeanRequestHandler< BucketAcl >
{
    public CreateBucketAclForUserRequestHandler()
    {
        super( BucketAcl.class,
               new BucketAuthorizationStrategy(
                    SystemBucketAccess.STANDARD,
                    BucketAclPermission.OWNER,
                    AdministratorOverride.YES ),
               RestDomainType.BUCKET_ACL,
               DefaultUserIdToUserMakingRequest.NO );

        registerRequiredBeanProperties( 
                BucketAcl.BUCKET_ID,
                UserIdObservable.USER_ID,
                BucketAcl.PERMISSION );
    }
}
