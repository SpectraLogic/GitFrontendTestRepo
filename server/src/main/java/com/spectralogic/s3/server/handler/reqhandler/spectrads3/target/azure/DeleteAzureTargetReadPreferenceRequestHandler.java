/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.target.AzureTargetReadPreference;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteAzureTargetReadPreferenceRequestHandler
    extends BaseDeleteBeanRequestHandler< AzureTargetReadPreference >
{
    public DeleteAzureTargetReadPreferenceRequestHandler()
    {
        super( AzureTargetReadPreference.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.AZURE_TARGET_READ_PREFERENCE );
    }
}
