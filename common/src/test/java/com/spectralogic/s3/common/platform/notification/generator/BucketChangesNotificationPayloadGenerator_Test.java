/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEventType;
import com.spectralogic.s3.common.dao.domain.notification.BucketNotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.notification.domain.payload.BucketChangesNotificationPayload;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.notification.dispatch.HttpNotifcationListener;
import com.spectralogic.util.notification.domain.DynamicNotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.Notification;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import java.util.Date;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;

public final class BucketChangesNotificationPayloadGenerator_Test 
{
    @Test
    public void testHappyConstruction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        new BucketChangesNotificationPayloadGenerator(
                0,
                dbSupport.getServiceManager().getRetriever( BucketHistoryEvent.class ) );
    }


    @Test
    public void testResponseIsCorrect()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "myBucket" );

        final BucketHistoryEvent change = BeanFactory.newBean(BucketHistoryEvent.class);
        change.setBucketId(bucket.getId());
        change.setObjectName("o1");
        change.setObjectCreationDate(new Date());
        change.setSequenceNumber(1L);
        change.setType(BucketHistoryEventType.CREATE);
        change.setVersionId(UUID.randomUUID());

        final BucketHistoryEvent change2 = BeanFactory.newBean(BucketHistoryEvent.class);
        change2.setBucketId(bucket.getId());
        change2.setObjectName("o1");
        change2.setObjectCreationDate(new Date());
        change2.setSequenceNumber(2L);
        change2.setType(BucketHistoryEventType.MARK_LATEST);
        change2.setVersionId(UUID.randomUUID());

        dbSupport.getServiceManager().getCreator(BucketHistoryEvent.class).create(change);
        dbSupport.getServiceManager().getCreator(BucketHistoryEvent.class).create(change2);

        final DynamicNotificationPayloadGenerator generator = new BucketChangesNotificationPayloadGenerator(
                1,
                dbSupport.getServiceManager().getRetriever( BucketHistoryEvent.class ) );

        final HttpNotifcationListener listener = new HttpNotifcationListener() {
            @Override
            public void fire(Notification event) {
                throw new IllegalStateException("Payload generation should not fire listener.");
            }

            @Override
            public BucketChangesNotificationRegistration getRegistration() {
                return BeanFactory.newBean(BucketChangesNotificationRegistration.class).setLastSequenceNumber(0L);
            }
        };
        final BucketChangesNotificationPayload event =
                (BucketChangesNotificationPayload)generator.generateNotificationPayload(listener);
        NotificationPayloadTracker.register( event );
        assertEquals(
                2,
                event.getChanges().length,
                "Shoulda returned correct response."
               );
    }
}
