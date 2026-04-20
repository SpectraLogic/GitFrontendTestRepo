/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.Ds3TargetFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDs3TargetFailureNotificationRegistrationRequestHandler
    extends BaseGetBeanRequestHandler< Ds3TargetFailureNotificationRegistration >
{
    public GetDs3TargetFailureNotificationRegistrationRequestHandler()
    {
        super( Ds3TargetFailureNotificationRegistration.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.DS3_TARGET_FAILURE_NOTIFICATION_REGISTRATION );
    }
}
