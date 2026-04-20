/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.target.S3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetS3TargetFailuresRequestHandler extends BaseGetBeansRequestHandler< S3TargetFailure >
{
    public GetS3TargetFailuresRequestHandler()
    {
        super( S3TargetFailure.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.S3_TARGET_FAILURE );
        
        registerOptionalBeanProperties( 
                TargetFailure.TARGET_ID, 
                Failure.TYPE,
                ErrorMessageObservable.ERROR_MESSAGE );
    }
}
