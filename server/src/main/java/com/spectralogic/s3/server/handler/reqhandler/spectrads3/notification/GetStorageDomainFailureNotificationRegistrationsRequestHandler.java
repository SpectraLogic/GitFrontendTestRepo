/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.StorageDomainFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetStorageDomainFailureNotificationRegistrationsRequestHandler
    extends BaseGetBeansRequestHandler< StorageDomainFailureNotificationRegistration >
{
    public GetStorageDomainFailureNotificationRegistrationsRequestHandler()
    {
        super( StorageDomainFailureNotificationRegistration.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.STORAGE_DOMAIN_FAILURE_NOTIFICATION_REGISTRATION );
        
        registerOptionalBeanProperties( UserIdObservable.USER_ID );
    }
}
