/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.AzureTargetFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteAzureTargetFailureNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< AzureTargetFailureNotificationRegistration >
{
    public DeleteAzureTargetFailureNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.AZURE_TARGET_FAILURE_NOTIFICATION_REGISTRATION,
               AzureTargetFailureNotificationRegistration.class );
    }
}
