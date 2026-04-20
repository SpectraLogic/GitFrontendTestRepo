/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.S3TargetBucketName;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateS3TargetBucketNameRequestHandler
    extends BaseCreateBeanRequestHandler< S3TargetBucketName >
{
    public CreateS3TargetBucketNameRequestHandler()
    {
        super( S3TargetBucketName.class,
               new BucketAuthorizationStrategy( 
                       SystemBucketAccess.STANDARD, BucketAclPermission.OWNER, AdministratorOverride.YES ),
               RestDomainType.S3_TARGET_BUCKET_NAME );
        
        registerBeanProperties( 
                PublicCloudTargetBucketName.BUCKET_ID,
                PublicCloudTargetBucketName.TARGET_ID,
                NameObservable.NAME );
    }
}
