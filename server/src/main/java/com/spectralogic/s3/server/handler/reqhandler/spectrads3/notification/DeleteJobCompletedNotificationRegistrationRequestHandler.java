/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteJobCompletedNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< JobCompletedNotificationRegistration >
{
    public DeleteJobCompletedNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.JOB_COMPLETED_NOTIFICATION_REGISTRATION,
               JobCompletedNotificationRegistration.class );
    }
}
