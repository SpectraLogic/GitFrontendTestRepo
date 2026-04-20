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
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetTapeDensityDirectiveRequestHandler 
    extends BaseGetBeanRequestHandler< TapeDensityDirective >
{
    public GetTapeDensityDirectiveRequestHandler()
    {
        super( TapeDensityDirective.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.TAPE_DENSITY_DIRECTIVE );
    }
}
