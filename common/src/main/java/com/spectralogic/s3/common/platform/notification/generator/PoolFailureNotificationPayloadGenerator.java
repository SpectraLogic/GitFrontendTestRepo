/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.platform.notification.domain.payload.PoolFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class PoolFailureNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public PoolFailureNotificationPayloadGenerator( final PoolFailure poolFailure )
    {
        Validations.verifyNotNull( "poolFailure", poolFailure );
        m_poolFailure = poolFailure;
    }


    @Override
    public NotificationPayload generateNotificationPayload()
    {
        return BeanFactory.newBean( PoolFailureNotificationPayload.class )
                .setDate( m_poolFailure.getDate() )
                .setPoolId( m_poolFailure.getPoolId() )
                .setType( m_poolFailure.getType() )
                .setErrorMessage( m_poolFailure.getErrorMessage() );
    }


    private final PoolFailure m_poolFailure;
}
