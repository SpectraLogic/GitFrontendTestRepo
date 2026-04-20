/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.AzureTargetFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetAzureTargetFailureNotificationRegistrationsRequestHandler
    extends BaseGetBeansRequestHandler< AzureTargetFailureNotificationRegistration >
{
    public GetAzureTargetFailureNotificationRegistrationsRequestHandler()
    {
        super( AzureTargetFailureNotificationRegistration.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.AZURE_TARGET_FAILURE_NOTIFICATION_REGISTRATION );
    
        registerOptionalBeanProperties( UserIdObservable.USER_ID );
    }
}
