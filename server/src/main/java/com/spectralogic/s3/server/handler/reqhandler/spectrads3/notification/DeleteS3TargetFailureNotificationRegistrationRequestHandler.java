/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3TargetFailureNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteS3TargetFailureNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< S3TargetFailureNotificationRegistration >
{
    public DeleteS3TargetFailureNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.S3_TARGET_FAILURE_NOTIFICATION_REGISTRATION,
               S3TargetFailureNotificationRegistration.class );
    }
}
