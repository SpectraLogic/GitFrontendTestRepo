/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.dispatch.bean;

import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.spectralogic.util.notification.dispatch.HttpNotifcationListener;
import com.spectralogic.util.notification.domain.*;
import com.spectralogic.util.notification.domain.bean.SequencedNotificationRegistrationObservable;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.BaseBeansRetriever;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.HttpResponseFormatType;
import com.spectralogic.util.http.HttpUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.notification.dispatch.NotificationListener;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.notification.dispatch.TransactionNotificationEventDispatcher;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;
import com.spectralogic.util.notification.domain.bean.HttpNotificationRegistration;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.WorkPool;

public final class HttpNotificationEventDispatcher implements NotificationEventDispatcher
{
    public HttpNotificationEventDispatcher( final WorkPool dispatchWorkPool )
    {
        m_dispatchWorkPool = dispatchWorkPool;
        Validations.verifyNotNull( "Work pool", m_dispatchWorkPool );
        LOG.info( LogUtil.getLogMessageImportantHeaderBlock( getClass().getSimpleName() + " Ready" ) );
    }


    public NotificationEventDispatcher startTransaction()
    {
        return new TransactionNotificationEventDispatcher( this );
    }


    public void commitTransaction()
    {
        throw new IllegalStateException( "Transaction not started." );
    }


    public void fire( final NotificationEvent< ? > fireEvent )
    {
        if ( !HttpNotificationEvent.class.isAssignableFrom( fireEvent.getClass() ) )
        {
            throw new UnsupportedOperationException( "Fire event not supported: " + fireEvent );
        }

        final HttpNotificationEvent event =
                (HttpNotificationEvent)fireEvent;

        fire(
                event.getNotificationReceiverRetriever(),
                event.getNotificationReceivers(),
                event.getEventGenerator(),
                event.getNotificationRegistrationType() );
    }

    public void queueFire( final NotificationEvent< ? > fireEvent )
    {
        m_singleThreadedWorkPool.submit( () -> fire( fireEvent ) );
    }


    void fire(
            final BeansRetriever< ? > notificationRegistrationService,
            final List< HttpNotificationRegistration< ? > > notificationRegistrationsToDispatchTo,
            final NotificationPayloadGenerator notificationEventGenerator,
            final Class< ? extends HttpNotificationRegistration< ? > > notificationRegistrationType )
    {
        Validations.verifyNotNull(
                "Notification event generator", notificationEventGenerator );
        Validations.verifyNotNull(
                "Notification registrations to dispatch to", notificationRegistrationsToDispatchTo );
        if ( !BeanUpdater.class.isAssignableFrom( notificationRegistrationService.getClass() ) )
        {
            throw new IllegalArgumentException(
                    "Must be able to update notification registrations: " + notificationRegistrationService );
        }

        if ( notificationRegistrationsToDispatchTo.isEmpty()
                && !m_localNotificationListeners.containsKey( notificationRegistrationType ) )
        {
            final Date lastLogged = m_logged_nobody_registered.get(notificationRegistrationType);
            final Date now = new Date();

            // Only log every 10 min
            if ( lastLogged == null || lastLogged.getTime() + ( 10 * 60L * 1000 ) < now.getTime() )
            {
                m_logged_nobody_registered.put(notificationRegistrationType, now);
                LOG.info( "Will not run " + notificationEventGenerator.getClass().getSimpleName()
                        + " since nobody is registered for the notification event." );
            }
            return;
        }

        final Set<NotificationListener> listeners =
                notificationRegistrationsToDispatchTo.stream().map( (it) -> new HttpNotificationListenerImpl<>(it, notificationRegistrationService)).collect(Collectors.toSet());
        if (m_localNotificationListeners.containsKey(notificationRegistrationType)) {
            listeners.addAll(m_localNotificationListeners.get( notificationRegistrationType ));
        }


        LOG.info( notificationEventGenerator.getClass().getSimpleName() + " was run since there are "
                + listeners.size()
                + " listeners registered for the notification event." );


        final CachingPayloadGenerator generator = new CachingPayloadGenerator(notificationEventGenerator);

        for ( final NotificationListener listener : listeners )
        {
            listener.fire( generator.generatePayload(listener) );
        }
    }

    private final static class CachingPayloadGenerator<T extends HttpNotificationRegistration> {

        public CachingPayloadGenerator( final NotificationPayloadGenerator generator ) {
            m_generator = generator;
        }

        public final Notification generatePayload( final NotificationListener registration) {
            if ( m_cachedNotification != null )
            {
                return m_cachedNotification;
            }

            final Notification notification;
            if (m_generator instanceof DynamicNotificationPayloadGenerator) {
                final NotificationPayload notificationPayload = ((DynamicNotificationPayloadGenerator)m_generator).generateNotificationPayload( registration );
                notification = payloadToNotification(notificationPayload);
            }
            else
            {
                final NotificationPayload notificationPayload = m_generator.generateNotificationPayload();
                notification = payloadToNotification(notificationPayload);
                m_cachedNotification = notification;
            }
            return notification;
        }

        private Notification payloadToNotification(NotificationPayload notificationPayload) {
            final Notification notification = BeanFactory.newBean( Notification.class );
            notification.setEvent( notificationPayload );
            notification.getEvent().setNotificationGenerationDate( new Date() );
            notification.setType( BeanFactory.getType(
                    notification.getEvent().getClass() ).getSimpleName() );
            return notification;
        }

        private Notification m_cachedNotification = null;
        private final NotificationPayloadGenerator m_generator;
    }


    private final static class EventDispatcher implements Runnable
    {
        private EventDispatcher(
                final NotificationContainer notification,
                final HttpNotificationRegistration< ? > registration,
                final BeansRetriever< ? > notificationRegistrationService )
        {
            m_notificationName = notification.getNotification().getType();
            m_registration = registration;
            m_requestMethod = registration.getNotificationHttpMethod().toString();
            m_targetUrl = registration.getNotificationEndPoint();
            final NotificationPayload event = notification.getNotification().getEvent();

            if ( event instanceof SequencedNotificationPayload ) {
                final SequencedEvent[] changes = ((SequencedNotificationPayload)event).getChanges();
                if (changes.length == 0)
                {
                    m_highestPayloadSequenceNumber = -1L;
                }
                else
                {
                    m_highestPayloadSequenceNumber = changes[changes.length - 1].getSequenceNumber();
                }
            } else {
                m_highestPayloadSequenceNumber = null;
            }
            final String notificationPayload = ( HttpResponseFormatType.JSON == registration.getFormat() ) ?
                    notification.toJson( registration.getNamingConvention() )
                    : notification.toXml( registration.getNamingConvention() );
            m_payload = notificationPayload;
            m_notificationRegistrationService = notificationRegistrationService;
        }


        public void run()
        {
            if (m_highestPayloadSequenceNumber != null && m_highestPayloadSequenceNumber == -1L) {
                //Do not send empty notification
                return;
            }

            final Duration duration = new Duration();
            HttpURLConnection connection = null;
            final MonitoredWork work = new MonitoredWork(
                    StackTraceLogging.SHORT, "Dispatch notification to " + m_targetUrl );
            try
            {
                final URL url = new URL( m_targetUrl );
                connection = (HttpURLConnection)url.openConnection();
                HttpUtil.hackConnectionForBadSslCertificate( connection );
                connection.setRequestMethod( m_requestMethod );
                final String contentType = ( HttpResponseFormatType.JSON == m_registration.getFormat() ) ?
                        "application/json"
                        : "application/xml";
                connection.setRequestProperty(
                        "Content-Length",
                        String.valueOf( m_payload.getBytes().length ) );
                connection.setRequestProperty(
                        "Content-Type",
                        contentType );
                connection.setRequestProperty(
                        "Accept",
                        contentType );

                connection.setUseCaches( false );
                connection.setDoOutput( true );

                final DataOutputStream wr = new DataOutputStream( connection.getOutputStream() );
                wr.write( m_payload.getBytes() );
                wr.flush();
                wr.close();

                final int httpResponseCode = connection.getResponseCode();
                update( Integer.valueOf( httpResponseCode ), null );
                LOG.info( "Successfully dispatched " + m_notificationName + " to '" + m_targetUrl
                          + "', which returned HTTP code " + httpResponseCode + " [" + duration + "]:"
                          + Platform.NEWLINE + m_payload );
            }
            catch ( final Exception ex )
            {
                LOG.info( "Failed to dispatch " + m_notificationName + " to '" + m_targetUrl
                          + "' [" + duration + "].", ex );
                update( null, ex.getMessage() );
                throw new RuntimeException( "Failed to dispatch notification to " + m_targetUrl + ".", ex );
            }
            finally
            {
                work.completed();
                if ( null != connection )
                {
                    connection.disconnect();
                }
            }
        }


        private void update( final Integer httpResponseCode, final String failure )
        {
            m_registration.setLastHttpResponseCode( httpResponseCode );
            m_registration.setLastNotification( new Date() );
            m_registration.setNumberOfFailuresSinceLastSuccess( ( null == httpResponseCode ) ?
                    m_registration.getNumberOfFailuresSinceLastSuccess() + 1 : 0 );
            m_registration.setLastFailure( failure );
            boolean updateSequenceNumber = false;
            if ( m_registration instanceof SequencedNotificationRegistrationObservable && null != httpResponseCode ) {
                ( (SequencedNotificationRegistrationObservable)m_registration ).setLastSequenceNumber( m_highestPayloadSequenceNumber );
                updateSequenceNumber = true;
            }

            try
            {
                @SuppressWarnings( "unchecked" )
                final BeanUpdater< DatabasePersistable > updater =
                        (BeanUpdater< DatabasePersistable >)m_notificationRegistrationService;
                if (updateSequenceNumber)
                {
                    updater.update(
                            (DatabasePersistable) m_registration,
                            HttpNotificationRegistration.LAST_HTTP_RESPONSE_CODE,
                            HttpNotificationRegistration.LAST_NOTIFICATION,
                            HttpNotificationRegistration.NUMBER_OF_FAILURES_SINCE_LAST_SUCCESS,
                            HttpNotificationRegistration.LAST_FAILURE,
                            SequencedNotificationRegistrationObservable.LAST_SEQUENCE_NUMBER);
                }
                else {
                    updater.update(
                            (DatabasePersistable) m_registration,
                            HttpNotificationRegistration.LAST_HTTP_RESPONSE_CODE,
                            HttpNotificationRegistration.LAST_NOTIFICATION,
                            HttpNotificationRegistration.NUMBER_OF_FAILURES_SINCE_LAST_SUCCESS,
                            HttpNotificationRegistration.LAST_FAILURE);
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to update notification registration.", ex );
            }
        }


        private final Long m_highestPayloadSequenceNumber;
        private final String m_payload;
        private final String m_targetUrl;
        private final String m_requestMethod;
        private final String m_notificationName;
        private final HttpNotificationRegistration< ? > m_registration;
        private final BeansRetriever< ? > m_notificationRegistrationService;
    } // end inner class def


    public void registerListener( final NotificationListener listener,
            final Class< ? extends HttpNotificationRegistration< ? > > notificationRegistrationType )
    {
        if ( !m_localNotificationListeners.containsKey( notificationRegistrationType ) )
        {
            m_localNotificationListeners.put( notificationRegistrationType, new HashSet<>() );
        }
        m_localNotificationListeners.get( notificationRegistrationType ).add( listener );
    }


    public void unregisterListener( final NotificationListener listener,
            final Class< ? extends HttpNotificationRegistration< ? > > notificationRegistrationType )
    {
        if ( !m_localNotificationListeners.containsKey( notificationRegistrationType )
                || !m_localNotificationListeners.get( notificationRegistrationType )
                        .contains( listener ) )
        {
            LOG.warn( "Cannot unregister \"" + listener + "\" for type \""
                    + notificationRegistrationType.getSimpleName() + "\" because no such registration exists." );
            return;
        }
        m_localNotificationListeners.get( notificationRegistrationType ).remove( listener );
        if ( m_localNotificationListeners.get( notificationRegistrationType ).isEmpty() )
        {
            m_localNotificationListeners.remove( notificationRegistrationType );
        }
    }

    private final class HttpNotificationListenerImpl<T extends HttpNotificationRegistration<?>> implements HttpNotifcationListener
    {
        public HttpNotificationListenerImpl( final T registration, final BeansRetriever< ? > notificationRegistrationService ) {
            m_registration = registration;
            m_notificationRegistrationService = notificationRegistrationService;
        }

        @Override
        public void fire(Notification event) {
            final NotificationContainer container = BeanFactory.newBean( NotificationContainer.class );
            container.setNotification( event );
            m_dispatchWorkPool.submit( new EventDispatcher(
                    container, m_registration, m_notificationRegistrationService ) );
        }

        @Override
        public T getRegistration() {
            return m_registration;
        }

        private final T m_registration;
        private final BeansRetriever< ? > m_notificationRegistrationService;
    }

    private final WorkPool m_dispatchWorkPool;
    private final WorkPool m_singleThreadedWorkPool = WorkPoolFactory.createWorkPool( 1, NotificationEventDispatcher.class.getSimpleName() );
    private final Map< Class< ? extends HttpNotificationRegistration< ? > >, Set<NotificationListener> >
                            m_localNotificationListeners = new HashMap<>();
    private final Map< Class< ? extends  HttpNotificationRegistration< ? >>, Date> m_logged_nobody_registered = new HashMap<>();
    private final static Logger LOG = Logger.getLogger( HttpNotificationEventDispatcher.class );
}