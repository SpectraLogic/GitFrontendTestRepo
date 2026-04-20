/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateGlobalBucketAclForUserRequestHandler
    extends BaseCreateBeanRequestHandler< BucketAcl >
{
    public CreateGlobalBucketAclForUserRequestHandler()
    {
        super( BucketAcl.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.BUCKET_ACL,
               DefaultUserIdToUserMakingRequest.NO );

        registerRequiredBeanProperties( 
                UserIdObservable.USER_ID,
                BucketAcl.PERMISSION );
    }
}
