/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.JobCreatedNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteJobCreatedNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< JobCreatedNotificationRegistration >
{
    public DeleteJobCreatedNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.JOB_CREATED_NOTIFICATION_REGISTRATION,
               JobCreatedNotificationRegistration.class );
    }
}
