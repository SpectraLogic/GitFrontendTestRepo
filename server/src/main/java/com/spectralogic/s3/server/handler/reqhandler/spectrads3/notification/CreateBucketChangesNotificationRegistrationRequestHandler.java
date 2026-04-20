/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

public final class CreateBucketChangesNotificationRegistrationRequestHandler
    extends BaseCreateBeanRequestHandler<BucketChangesNotificationRegistration>
{
    public CreateBucketChangesNotificationRegistrationRequestHandler()
    {
        super( BucketChangesNotificationRegistration.class,
               new BucketAuthorizationStrategy(
                    SystemBucketAccess.STANDARD, 
                    BucketAclPermission.JOB,
                    AdministratorOverride.YES ),
               RestDomainType.BUCKET_CHANGES_NOTIFICATION_REGISTRATION,
               DefaultUserIdToUserMakingRequest.YES );
        
        registerBeanProperties(
                HttpNotificationRegistration.FORMAT,
                HttpNotificationRegistration.NAMING_CONVENTION,
                HttpNotificationRegistration.NOTIFICATION_END_POINT,
                HttpNotificationRegistration.NOTIFICATION_HTTP_METHOD,
                BucketChangesNotificationRegistration.BUCKET_ID);
    }

    @Override
    protected void validateBeanForCreation( final CommandExecutionParams params, final BucketChangesNotificationRegistration registration ) {
        final long maxSequenceNumber = params.getServiceManager().getRetriever( BucketHistoryEvent.class )
                .getMax( BucketHistoryEvent.SEQUENCE_NUMBER, Require.nothing() );
        registration.setLastSequenceNumber(maxSequenceNumber);
    }
}
