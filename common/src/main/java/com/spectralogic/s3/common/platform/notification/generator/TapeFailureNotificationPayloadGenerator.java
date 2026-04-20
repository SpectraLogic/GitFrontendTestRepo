/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.platform.notification.domain.payload.TapeFailureNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class TapeFailureNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public TapeFailureNotificationPayloadGenerator( final TapeFailure tapeFailure )
    {
        Validations.verifyNotNull( "tapeFailure", tapeFailure );
        m_tapeFailure = tapeFailure;
    }


    @Override
    public NotificationPayload generateNotificationPayload()
    {
        return BeanFactory.newBean( TapeFailureNotificationPayload.class )
                .setDate( m_tapeFailure.getDate() )
                .setTapeDriveId( m_tapeFailure.getTapeDriveId() )
                .setTapeId( m_tapeFailure.getTapeId() )
                .setType( m_tapeFailure.getType() )
                .setErrorMessage( m_tapeFailure.getErrorMessage() );
    }


    private final TapeFailure m_tapeFailure;
}
