/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteObjectPersistedNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< S3ObjectPersistedNotificationRegistration >
{
    public DeleteObjectPersistedNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.OBJECT_PERSISTED_NOTIFICATION_REGISTRATION,
               S3ObjectPersistedNotificationRegistration.class );
    }
}
