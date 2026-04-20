/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.platform.notification.domain.payload.AzureTargetFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class AzureTargetFailureNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public AzureTargetFailureNotificationPayloadGenerator( final AzureTargetFailure targetFailure )
    {
        Validations.verifyNotNull( "targetFailure", targetFailure );
        m_targetFailure = targetFailure;
    }


    @Override
    public NotificationPayload generateNotificationPayload()
    {
        return BeanFactory.newBean( AzureTargetFailureNotificationPayload.class )
                .setDate( m_targetFailure.getDate() )
                .setTargetId( m_targetFailure.getTargetId() )
                .setType( m_targetFailure.getType() )
                .setErrorMessage( m_targetFailure.getErrorMessage() );
    }


    private final AzureTargetFailure m_targetFailure;
}
