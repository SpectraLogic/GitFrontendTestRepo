/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.event;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.notification.BucketNotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.domain.notification.NotificationRegistrationObservable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;

public final class BucketNotificationEvent extends HttpNotificationEvent
{
    public BucketNotificationEvent(
            final Bucket bucket,
            final BeansRetriever< ? extends NotificationRegistrationObservable< ? > > notificationRegRetriever,
            final NotificationPayloadGenerator notificationEventGenerator )
    {
        super( notificationRegRetriever, notificationEventGenerator );
        m_bucket = bucket;
        Validations.verifyNotNull( "Bucket", bucket );
    }
    
    
    @Override
    protected WhereClause getNotificationRegistrationObservableFilter()
    {
        final WhereClause permissionsFilter = Require.any( 
                Require.beanPropertyEquals( 
                        UserIdObservable.USER_ID,
                        m_bucket.getUserId() ),
                Require.beanPropertyEquals( 
                        UserIdObservable.USER_ID,
                        null ) );
        
        final WhereClause bucketFilter;
        if ( !BucketNotificationRegistrationObservable.class.isAssignableFrom( m_retriever.getServicedType() ) )
        {
            bucketFilter = null;
        }
        else
        {
            bucketFilter = Require.any(
                    Require.beanPropertyEquals( 
                            BucketNotificationRegistrationObservable.BUCKET_ID,
                            m_bucket.getId() ),
                    Require.beanPropertyEquals( 
                            BucketNotificationRegistrationObservable.BUCKET_ID,
                            null ) );
        }
        
        return Require.all( permissionsFilter, bucketFilter );
    }


    private final Bucket m_bucket;
}
