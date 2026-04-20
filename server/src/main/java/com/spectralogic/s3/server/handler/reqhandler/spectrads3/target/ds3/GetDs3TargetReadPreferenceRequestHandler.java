/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import com.spectralogic.s3.common.dao.domain.target.Ds3TargetReadPreference;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDs3TargetReadPreferenceRequestHandler
    extends BaseGetBeanRequestHandler< Ds3TargetReadPreference >
{
    public GetDs3TargetReadPreferenceRequestHandler()
    {
        super( Ds3TargetReadPreference.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DS3_TARGET_READ_PREFERENCE );
    }
}