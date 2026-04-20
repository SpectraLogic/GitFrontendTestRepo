/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetTapeDensityDirectivesRequestHandler 
    extends BaseGetBeansRequestHandler< TapeDensityDirective >
{
    public GetTapeDensityDirectivesRequestHandler()
    {
        super( TapeDensityDirective.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.TAPE_DENSITY_DIRECTIVE );
        
        registerOptionalBeanProperties(
                TapeDensityDirective.DENSITY,
                TapeDensityDirective.PARTITION_ID,
                TapeDensityDirective.TAPE_TYPE );
    }
}
