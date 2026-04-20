/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.target.S3TargetReadPreference;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteS3TargetReadPreferenceRequestHandler
    extends BaseDeleteBeanRequestHandler< S3TargetReadPreference >
{
    public DeleteS3TargetReadPreferenceRequestHandler()
    {
        super( S3TargetReadPreference.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.S3_TARGET_READ_PREFERENCE );
    }
}
