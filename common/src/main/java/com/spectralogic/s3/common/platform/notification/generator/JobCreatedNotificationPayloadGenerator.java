/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.UUID;

import com.spectralogic.s3.common.platform.notification.domain.payload.JobCreatedNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public final class JobCreatedNotificationPayloadGenerator implements NotificationPayloadGenerator
{
    public JobCreatedNotificationPayloadGenerator( final UUID jobId )
    {
        m_jobId = jobId;
        Validations.verifyNotNull( "Job", m_jobId );
    }

    
    public NotificationPayload generateNotificationPayload()
    {
        final JobCreatedNotificationPayload retval = 
                BeanFactory.newBean( JobCreatedNotificationPayload.class );
        retval.setJobId( m_jobId );
        return retval;
    }
    
    
    private final UUID m_jobId;
}
