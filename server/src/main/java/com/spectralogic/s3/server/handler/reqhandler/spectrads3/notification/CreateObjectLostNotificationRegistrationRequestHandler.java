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
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public final class CreateObjectLostNotificationRegistrationRequestHandler
    extends BaseCreateBeanRequestHandler< S3ObjectLostNotificationRegistration >
{
    public CreateObjectLostNotificationRegistrationRequestHandler()
    {
        super( S3ObjectLostNotificationRegistration.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.OBJECT_LOST_NOTIFICATION_REGISTRATION,
               DefaultUserIdToUserMakingRequest.YES );
        
        registerBeanProperties( 
                HttpNotificationRegistration.FORMAT,
                HttpNotificationRegistration.NAMING_CONVENTION,
                HttpNotificationRegistration.NOTIFICATION_END_POINT,
                HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD );
    }
}
