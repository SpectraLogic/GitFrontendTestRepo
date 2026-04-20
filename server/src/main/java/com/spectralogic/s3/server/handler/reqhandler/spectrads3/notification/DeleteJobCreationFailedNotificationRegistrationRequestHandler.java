/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.JobCreationFailedNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteJobCreationFailedNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< JobCreationFailedNotificationRegistration >
{
    public DeleteJobCreationFailedNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.JOB_CREATION_FAILED_NOTIFICATION_REGISTRATION,
               JobCreationFailedNotificationRegistration.class );
    }
}
