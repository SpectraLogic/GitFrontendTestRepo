/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.event;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.JobNotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.domain.notification.NotificationRegistrationObservable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;

public final class JobNotificationEvent extends HttpNotificationEvent
{
    public JobNotificationEvent(
            final Job job,
            final BeansRetriever< ? extends NotificationRegistrationObservable< ? > > notifcationRegRetriever,
            final NotificationPayloadGenerator notificationEventGenerator )
    {
        super( notifcationRegRetriever, notificationEventGenerator );
        m_job = job;
        Validations.verifyNotNull( "Job", job );
    }
    
    
    @Override
    protected WhereClause getNotificationRegistrationObservableFilter()
    {
        final WhereClause permissionsFilter = Require.any( 
                Require.beanPropertyEquals( 
                        UserIdObservable.USER_ID,
                        m_job.getUserId() ),
                Require.beanPropertyEquals( 
                        UserIdObservable.USER_ID,
                        null ) );
        
        final WhereClause jobFilter;
        if ( !JobNotificationRegistrationObservable.class.isAssignableFrom( m_retriever.getServicedType() ) )
        {
            jobFilter = null;
        }
        else
        {
            jobFilter = Require.any(  
                    Require.beanPropertyEquals( 
                            JobNotificationRegistrationObservable.JOB_ID,
                            m_job.getId() ),
                    Require.beanPropertyEquals( 
                            JobNotificationRegistrationObservable.JOB_ID,
                            null ) );
        }
        
        return Require.all( permissionsFilter, jobFilter );
    }


    private final Job m_job;
}
