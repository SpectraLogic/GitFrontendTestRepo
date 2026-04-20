/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.notification;

import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.GenericDaoNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetGenericDaoNotificationRegistrationsRequestHandler
    extends BaseGetBeansRequestHandler< GenericDaoNotificationRegistration >
{
    public GetGenericDaoNotificationRegistrationsRequestHandler()
    {
        super( GenericDaoNotificationRegistration.class,
                new InternalAccessOnlyAuthenticationStrategy(),
                RestDomainType.GENERIC_DAO_NOTIFICATION_REGISTRATION );
        
        registerOptionalBeanProperties( UserIdObservable.USER_ID );
    }
}
