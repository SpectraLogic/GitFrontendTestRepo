/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.Ds3TargetFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteDs3TargetFailureNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< Ds3TargetFailureNotificationRegistration >
{
    public DeleteDs3TargetFailureNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.DS3_TARGET_FAILURE_NOTIFICATION_REGISTRATION,
               Ds3TargetFailureNotificationRegistration.class );
    }
}
