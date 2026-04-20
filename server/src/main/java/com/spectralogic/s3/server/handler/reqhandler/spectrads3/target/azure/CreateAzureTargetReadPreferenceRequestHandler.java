/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.target.AzureTargetReadPreference;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreference;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateAzureTargetReadPreferenceRequestHandler
    extends BaseCreateBeanRequestHandler< AzureTargetReadPreference >
{
    public CreateAzureTargetReadPreferenceRequestHandler()
    {
        super( AzureTargetReadPreference.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.AZURE_TARGET_READ_PREFERENCE );
        
        registerBeanProperties( 
                TargetReadPreference.BUCKET_ID,
                TargetReadPreference.READ_PREFERENCE,
                TargetReadPreference.TARGET_ID );
    }
}
