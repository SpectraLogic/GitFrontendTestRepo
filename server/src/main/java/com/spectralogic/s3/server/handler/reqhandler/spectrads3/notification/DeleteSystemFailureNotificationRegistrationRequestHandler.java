/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.SystemFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteSystemFailureNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< SystemFailureNotificationRegistration >
{
    public DeleteSystemFailureNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.SYSTEM_FAILURE_NOTIFICATION_REGISTRATION,
                SystemFailureNotificationRegistration.class );
    }
}
