/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.notification;

import com.spectralogic.s3.common.dao.domain.notification.GenericDaoNotificationRegistration;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteGenericDaoNotificationRegistrationRequestHandler
    extends BaseDeleteBeanRequestHandler< GenericDaoNotificationRegistration >
{
    public DeleteGenericDaoNotificationRegistrationRequestHandler()
    {
        super( GenericDaoNotificationRegistration.class,
                new InternalAccessOnlyAuthenticationStrategy(),
                RestDomainType.GENERIC_DAO_NOTIFICATION_REGISTRATION );
    }
}
