/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.notification;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;

public interface JobNotificationRegistrationObservable 
    extends NotificationRegistrationObservable< JobNotificationRegistrationObservable >
{
    String JOB_ID = "jobId";
    
    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Job.class )
    UUID getJobId();
    
    void setJobId( final UUID value );
}
