/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.target.S3TargetReadPreference;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreference;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetS3TargetReadPreferencesRequestHandler
    extends BaseGetBeansRequestHandler< S3TargetReadPreference >
{
    public GetS3TargetReadPreferencesRequestHandler()
    {
        super( S3TargetReadPreference.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.S3_TARGET_READ_PREFERENCE );
    
        registerOptionalBeanProperties(
                TargetReadPreference.BUCKET_ID,
                TargetReadPreference.READ_PREFERENCE,
                TargetReadPreference.TARGET_ID );
    }
}
