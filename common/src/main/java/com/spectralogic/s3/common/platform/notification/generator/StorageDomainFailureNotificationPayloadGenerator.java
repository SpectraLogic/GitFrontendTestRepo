/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.platform.notification.domain.payload.StorageDomainFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class StorageDomainFailureNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public StorageDomainFailureNotificationPayloadGenerator( final StorageDomainFailure failure )
    {
        Validations.verifyNotNull( "failure", failure );
        m_failure = failure;
    }


    @Override
    public NotificationPayload generateNotificationPayload()
    {
        return BeanFactory.newBean( StorageDomainFailureNotificationPayload.class )
                .setDate( m_failure.getDate() )
                .setStorageDomainId( m_failure.getStorageDomainId() )
                .setType( m_failure.getType() )
                .setErrorMessage( m_failure.getErrorMessage() );
    }


    private final StorageDomainFailure m_failure;
}
