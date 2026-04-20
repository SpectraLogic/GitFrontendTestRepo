/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.platform.notification.domain.payload.SystemFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class SystemFailureNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public SystemFailureNotificationPayloadGenerator( final SystemFailure failure )
    {
        Validations.verifyNotNull( "failure", failure );
        m_failure = failure;
    }


    @Override
    public NotificationPayload generateNotificationPayload()
    {
        return BeanFactory.newBean( SystemFailureNotificationPayload.class )
                .setDate( m_failure.getDate() )
                .setType( m_failure.getType() )
                .setErrorMessage( m_failure.getErrorMessage() );
    }


    private final SystemFailure m_failure;
}