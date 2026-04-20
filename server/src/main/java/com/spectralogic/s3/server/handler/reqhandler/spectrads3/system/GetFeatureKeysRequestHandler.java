/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetFeatureKeysRequestHandler extends BaseGetBeansRequestHandler< FeatureKey >
{
    public GetFeatureKeysRequestHandler()
    {
        super( FeatureKey.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.NONE ),
               RestDomainType.FEATURE_KEY );
        
        registerOptionalBeanProperties( 
                ErrorMessageObservable.ERROR_MESSAGE,
                FeatureKey.KEY, 
                FeatureKey.EXPIRATION_DATE );
    }
}
