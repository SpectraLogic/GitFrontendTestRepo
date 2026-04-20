/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.NotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.notification.domain.event.JobNotificationEvent;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.notification.dispatch.bean.HttpNotificationEventDispatcher;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class NotificationEventIntegration_Test 
{
    @Test
    public void testAutoDiscoverDispatchNotificationRegistrationsDoesNotCallToGenerateEventWhenNobdyToNotify()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansRetriever< ? extends NotificationRegistrationObservable< ? > > service =
                dbSupport.getServiceManager().getRetriever( JobCompletedNotificationRegistration.class );
        
        final User user = mockDaoDriver.createUser( "user1" );
        final User user2 = mockDaoDriver.createUser( "user2" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "somebucket" );
        
        final Job job =
                BeanFactory.newBean( Job.class ).setUserId( user.getId() ).setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( job );
        
        final Job job2 =
                BeanFactory.newBean( Job.class ).setUserId( user.getId() ).setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( job2 );
        
        final JobCompletedNotificationRegistration registration =
                BeanFactory.newBean( JobCompletedNotificationRegistration.class );
        registration.setNotificationEndPoint( "a" );
        registration.setJobId( job.getId() );
        registration.setUserId( user2.getId() );
        dbSupport.getDataManager().createBean( registration );
        
        final JobCompletedNotificationRegistration registration2 =
                BeanFactory.newBean( JobCompletedNotificationRegistration.class );
        registration2.setNotificationEndPoint( "a" );
        registration2.setJobId( job2.getId() );
        dbSupport.getDataManager().createBean( registration2 );

        final MockNotificationEventGenerator generator = new MockNotificationEventGenerator();
        new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() ).fire( 
                new JobNotificationEvent( 
                        job,
                        service,
                        generator ) );
        assertEquals(
                0,
                generator.m_callCount.get(),
                "Should notta asked generator to generate the notification since no listeners."
                );
    }
    
    
    @Test
    public void testAutoDiscoverDispatchNotificationRegistrationsDoesCallToGenerateEventWhenSomebodyToNotify()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansRetriever< ? extends NotificationRegistrationObservable< ? > > service =
                dbSupport.getServiceManager().getRetriever( JobCompletedNotificationRegistration.class );

        final User user = mockDaoDriver.createUser( "user1" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "somebucket" );
        
        final Job job =
                BeanFactory.newBean( Job.class ).setUserId( user.getId() ).setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( job );
        
        final Job job2 =
                BeanFactory.newBean( Job.class ).setUserId( user.getId() ).setBucketId( bucket.getId() )
                .setRequestType( JobRequestType.values()[ 0 ] );
        dbSupport.getDataManager().createBean( job2 );
        
        final JobCompletedNotificationRegistration registration =
                BeanFactory.newBean( JobCompletedNotificationRegistration.class );
        registration.setNotificationEndPoint( "a" );
        registration.setJobId( job.getId() );
        registration.setUserId( user.getId() );
        dbSupport.getDataManager().createBean( registration );

        final MockNotificationEventGenerator generator = new MockNotificationEventGenerator();
        new HttpNotificationEventDispatcher( SystemWorkPool.getInstance() ).fire(
                new JobNotificationEvent( 
                        job,
                        service,
                        generator ) );
        assertEquals(
                1,
                generator.m_callCount.get(),
                "Should notta asked generator to generate the notification since no listeners."
                );
    }
    
    
    private final static class MockNotificationEventGenerator implements NotificationPayloadGenerator
    {
        public NotificationPayload generateNotificationPayload()
        {
            m_callCount.incrementAndGet();
            return BeanFactory.newBean( TestBean.class ).setIntProp( 999 );
        }
        
        private final AtomicInteger m_callCount = new AtomicInteger();
    } // end inner class def
}
