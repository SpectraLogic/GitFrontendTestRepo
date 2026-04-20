/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import java.util.UUID;

import com.spectralogic.util.notification.domain.NotificationPayload;

public interface JobCreatedNotificationPayload extends NotificationPayload
{
    String JOB_ID = "jobId";
    
    UUID getJobId();
    
    void setJobId( final UUID value );
}
