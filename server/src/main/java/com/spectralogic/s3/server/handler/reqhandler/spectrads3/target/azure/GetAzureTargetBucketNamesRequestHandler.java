/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetBucketName;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetAzureTargetBucketNamesRequestHandler
    extends BaseGetBeansRequestHandler< AzureTargetBucketName >
{
    public GetAzureTargetBucketNamesRequestHandler()
    {
        super( AzureTargetBucketName.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ), 
               RestDomainType.AZURE_TARGET_BUCKET_NAME );
        
        registerOptionalBeanProperties( 
                PublicCloudTargetBucketName.BUCKET_ID,
                PublicCloudTargetBucketName.TARGET_ID,
                NameObservable.NAME );
    }
}
