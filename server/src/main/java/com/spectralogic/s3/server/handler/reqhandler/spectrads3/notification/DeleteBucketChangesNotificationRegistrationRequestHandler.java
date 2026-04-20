/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.UserIdObservableDeleteRequestHandler;
//import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteBucketChangesNotificationRegistrationRequestHandler
    extends UserIdObservableDeleteRequestHandler< BucketChangesNotificationRegistration >
{
    public DeleteBucketChangesNotificationRegistrationRequestHandler()
    {
        super( RestDomainType.BUCKET_CHANGES_NOTIFICATION_REGISTRATION,
               BucketChangesNotificationRegistration.class );
    }
}
