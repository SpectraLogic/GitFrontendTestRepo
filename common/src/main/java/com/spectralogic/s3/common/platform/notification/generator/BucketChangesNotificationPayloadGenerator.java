package com.spectralogic.s3.common.platform.notification.generator;

import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.platform.notification.domain.payload.BucketChangesNotificationPayload;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseBeansRetriever;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.notification.dispatch.HttpNotifcationListener;
import com.spectralogic.util.notification.dispatch.NotificationListener;
import com.spectralogic.util.notification.domain.DynamicNotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;

import java.lang.reflect.Method;
import java.util.List;

public class BucketChangesNotificationPayloadGenerator implements DynamicNotificationPayloadGenerator {

    public BucketChangesNotificationPayloadGenerator(
            final long minSequenceNumber,
            final BeansRetriever<BucketHistoryEvent> historyRetriever
    ) {
        m_sequenceNumber = minSequenceNumber;
        m_historyRetriever = extractNonTransactionHistoryRetriever(historyRetriever);
    }

    private <T extends SimpleBeanSafeToProxy>BeansRetriever< T > extractNonTransactionHistoryRetriever(
            final BeansRetriever< T > historyRetriever )
    {
        try
        {
            final BaseService< ? > service = (BaseService< ? >)historyRetriever;
            final Method mGetServiceManager =
                    BaseBeansRetriever.class.getDeclaredMethod( "getServiceManager" );
            mGetServiceManager.setAccessible( true );
            final BeansServiceManager serviceManager =
                    (BeansServiceManager)mGetServiceManager.invoke( service );
            if ( !serviceManager.isTransaction() )
            {
                return historyRetriever;
            }
            return serviceManager.getTransactionSource().getRetriever(
                    historyRetriever.getServicedType() );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException(
                    "Cannot extract non-transaction notification retriever service for: "
                            + historyRetriever.getClass().getName(), ex );
        }
    }

    @Override
    public NotificationPayload generateNotificationPayload( NotificationListener listener )
    {
        if ( listener instanceof HttpNotifcationListener ) {
            HttpNotificationRegistration registration = ((HttpNotifcationListener)listener).getRegistration();
            if (registration instanceof BucketChangesNotificationRegistration)
            {
                final BucketChangesNotificationPayload retval = BeanFactory.newBean(BucketChangesNotificationPayload.class);
                final BucketChangesNotificationRegistration bucketHistoryRegistration = (BucketChangesNotificationRegistration) registration;
                final WhereClause bucketFilter;

                if ( bucketHistoryRegistration.getBucketId() == null )
                {
                    bucketFilter = Require.nothing();
                }
                else
                {
                    bucketFilter = Require.beanPropertyEquals(BucketHistoryEvent.BUCKET_ID, bucketHistoryRegistration.getBucketId());
                }
                final WhereClause sequenceFilter;
                if (bucketHistoryRegistration.getLastSequenceNumber() == null)
                {
                    sequenceFilter = Require.nothing();
                }
                else if (bucketHistoryRegistration.getLastSequenceNumber() > m_sequenceNumber)
                {
                    //NOTE: if the earliest notification we are trying to send is somehow earlier than the last processed
                    //notification it means we missed this one in a previous update. We'll resend everything back to this point.
                    sequenceFilter = Require.beanPropertyGreaterThan(BucketHistoryEvent.SEQUENCE_NUMBER, m_sequenceNumber - 1);
                }
                else
                {
                    sequenceFilter = Require.beanPropertyGreaterThan(BucketHistoryEvent.SEQUENCE_NUMBER, bucketHistoryRegistration.getLastSequenceNumber());
                }

                final List<BucketHistoryEvent> eventsToSend = m_historyRetriever.retrieveAll(Require.all(sequenceFilter, bucketFilter)).toList();
                retval.setChanges(eventsToSend.toArray(new BucketHistoryEvent[0]));
                retval.setLastProcessedEvent(bucketHistoryRegistration.getLastSequenceNumber());
                return retval;
            }
            else
            {
                throw new UnsupportedOperationException("Cannot generate dynamic payload for registration of type: " + registration.getClass().getSimpleName());
            }
        }
        else {
            throw new UnsupportedOperationException("Cannot generate dynamic payload for listener of type: " + listener.getClass().getSimpleName());
        }
    }

    private final long m_sequenceNumber;
    private final BeansRetriever<BucketHistoryEvent> m_historyRetriever;
}