/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.notification.JobNotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public final class CreateObjectCachedNotificationRegistrationRequestHandler
    extends BaseCreateBeanRequestHandler< S3ObjectCachedNotificationRegistration >
{
    public CreateObjectCachedNotificationRegistrationRequestHandler()
    {
        super( S3ObjectCachedNotificationRegistration.class, 
               new BucketAuthorizationStrategy(
                    SystemBucketAccess.STANDARD, 
                    BucketAclPermission.JOB,
                    AdministratorOverride.YES ),
               RestDomainType.OBJECT_CACHED_NOTIFICATION_REGISTRATION,
               DefaultUserIdToUserMakingRequest.YES );
        
        registerBeanProperties( 
                JobNotificationRegistrationObservable.JOB_ID,
                HttpNotificationRegistration.FORMAT,
                HttpNotificationRegistration.NAMING_CONVENTION,
                HttpNotificationRegistration.NOTIFICATION_END_POINT,
                HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD );
    }
}
