/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetObjectPersistedNotificationRegistrationsRequestHandler
    extends BaseGetBeansRequestHandler< S3ObjectPersistedNotificationRegistration >
{
    public GetObjectPersistedNotificationRegistrationsRequestHandler()
    {
        super( S3ObjectPersistedNotificationRegistration.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.OBJECT_PERSISTED_NOTIFICATION_REGISTRATION );
        
        registerOptionalBeanProperties( UserIdObservable.USER_ID );
    }
}
