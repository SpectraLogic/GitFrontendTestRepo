/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetBucketName;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateAzureTargetBucketNameRequestHandler
    extends BaseCreateBeanRequestHandler< AzureTargetBucketName >
{
    public CreateAzureTargetBucketNameRequestHandler()
    {
        super( AzureTargetBucketName.class,
               new BucketAuthorizationStrategy( 
                       SystemBucketAccess.STANDARD, BucketAclPermission.OWNER, AdministratorOverride.YES ),
               RestDomainType.AZURE_TARGET_BUCKET_NAME );
        
        registerBeanProperties( 
                PublicCloudTargetBucketName.BUCKET_ID,
                PublicCloudTargetBucketName.TARGET_ID,
                NameObservable.NAME );
    }
}
