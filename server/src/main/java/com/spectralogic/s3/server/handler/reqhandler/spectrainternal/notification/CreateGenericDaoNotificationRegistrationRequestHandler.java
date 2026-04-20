/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.notification;

import com.spectralogic.s3.common.dao.domain.notification.GenericDaoNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public final class CreateGenericDaoNotificationRegistrationRequestHandler
    extends BaseCreateBeanRequestHandler< GenericDaoNotificationRegistration >
{
    public CreateGenericDaoNotificationRegistrationRequestHandler()
    {
        super( GenericDaoNotificationRegistration.class, 
               new InternalAccessOnlyAuthenticationStrategy(),
               RestDomainType.GENERIC_DAO_NOTIFICATION_REGISTRATION,
               DefaultUserIdToUserMakingRequest.YES );
        
        registerBeanProperties( 
                GenericDaoNotificationRegistration.DAO_TYPE,
                HttpNotificationRegistration.FORMAT,
                HttpNotificationRegistration.NAMING_CONVENTION,
                HttpNotificationRegistration.NOTIFICATION_END_POINT,
                HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD );
    }
}
