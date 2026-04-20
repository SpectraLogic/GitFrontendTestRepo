/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetAzureTargetFailuresRequestHandler 
    extends BaseGetBeansRequestHandler< AzureTargetFailure >
{
    public GetAzureTargetFailuresRequestHandler()
    {
        super( AzureTargetFailure.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.AZURE_TARGET_FAILURE );
        
        registerOptionalBeanProperties( 
                TargetFailure.TARGET_ID, 
                Failure.TYPE,
                ErrorMessageObservable.ERROR_MESSAGE );
    }
}
