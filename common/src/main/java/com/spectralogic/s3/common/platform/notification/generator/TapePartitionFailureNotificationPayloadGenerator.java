/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.platform.notification.domain.payload.TapePartitionFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class TapePartitionFailureNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public TapePartitionFailureNotificationPayloadGenerator( final TapePartitionFailure tapePartitionFailure )
    {
        Validations.verifyNotNull( "tapePartitionFailure", tapePartitionFailure );
        m_tapePartitionFailure = tapePartitionFailure;
    }


    @Override
    public NotificationPayload generateNotificationPayload()
    {
        return BeanFactory.newBean( TapePartitionFailureNotificationPayload.class )
                .setDate( m_tapePartitionFailure.getDate() )
                .setPartitionId( m_tapePartitionFailure.getPartitionId() )
                .setType( m_tapePartitionFailure.getType() )
                .setErrorMessage( m_tapePartitionFailure.getErrorMessage() );
    }


    private final TapePartitionFailure m_tapePartitionFailure;
}
