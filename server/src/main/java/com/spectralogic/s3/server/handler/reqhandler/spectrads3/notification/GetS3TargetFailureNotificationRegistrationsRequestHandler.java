/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.S3TargetFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetS3TargetFailureNotificationRegistrationsRequestHandler
    extends BaseGetBeansRequestHandler< S3TargetFailureNotificationRegistration >
{
    public GetS3TargetFailureNotificationRegistrationsRequestHandler()
    {
        super( S3TargetFailureNotificationRegistration.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.S3_TARGET_FAILURE_NOTIFICATION_REGISTRATION );
    
        registerOptionalBeanProperties( UserIdObservable.USER_ID );
    }
}
