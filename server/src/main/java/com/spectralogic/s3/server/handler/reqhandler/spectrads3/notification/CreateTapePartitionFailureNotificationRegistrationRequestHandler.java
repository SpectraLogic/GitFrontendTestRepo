/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.TapePartitionFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public final class CreateTapePartitionFailureNotificationRegistrationRequestHandler
    extends BaseCreateBeanRequestHandler< TapePartitionFailureNotificationRegistration >
{
    public CreateTapePartitionFailureNotificationRegistrationRequestHandler()
    {
        super( TapePartitionFailureNotificationRegistration.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.TAPE_PARTITION_FAILURE_NOTIFICATION_REGISTRATION,
               DefaultUserIdToUserMakingRequest.YES );
        
        registerBeanProperties( 
                HttpNotificationRegistration.FORMAT,
                HttpNotificationRegistration.NAMING_CONVENTION,
                HttpNotificationRegistration.NOTIFICATION_END_POINT,
                HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD );
    }
}
