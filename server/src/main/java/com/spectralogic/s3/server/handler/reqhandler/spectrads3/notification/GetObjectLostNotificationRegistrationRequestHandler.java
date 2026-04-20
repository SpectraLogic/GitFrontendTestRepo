/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectLostNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetObjectLostNotificationRegistrationRequestHandler
    extends BaseGetBeanRequestHandler< S3ObjectLostNotificationRegistration >
{
    public GetObjectLostNotificationRegistrationRequestHandler()
    {
        super( S3ObjectLostNotificationRegistration.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.OBJECT_LOST_NOTIFICATION_REGISTRATION );
    }
}
