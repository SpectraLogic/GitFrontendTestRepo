/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain.bean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseBeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;

public class HttpNotificationEvent
    implements BeanNotificationEvent< HttpNotificationRegistration< ? > >
{
    public HttpNotificationEvent(
            final BeansRetriever< ? extends HttpNotificationRegistration< ? > > notificationRegRetriever,
            final NotificationPayloadGenerator notificationEventGenerator )
    {
Validations.verifyNotNull( "Notification registration retriever", notificationRegRetriever );
        Validations.verifyNotNull( "Event generator", notificationEventGenerator );
        @SuppressWarnings( "unchecked" )
        final BeansRetriever< HttpNotificationRegistration< ? > > castedRetriever =
                (BeansRetriever< HttpNotificationRegistration< ? > >)notificationRegRetriever;
        m_retriever = extractNonTransactionNotificationRegistrationService( castedRetriever );
        m_eventGenerator = notificationEventGenerator;
    }
    
    
    final public BeansRetriever< HttpNotificationRegistration< ? > > getNotificationReceiverRetriever()
    {
        return m_retriever;
    }
    
    
    protected WhereClause getNotificationRegistrationObservableFilter()
    {
        return Require.nothing();
    }
    
    
    final public List< HttpNotificationRegistration< ? > > getNotificationReceivers()
    {
        return new ArrayList<>(
                m_retriever.retrieveAll( getNotificationRegistrationObservableFilter() ).toList() );
    }
    
    
    final public Class< ? extends HttpNotificationRegistration< ? > > getNotificationRegistrationType()
    {
        return m_retriever.getServicedType();
    }


    final public NotificationPayloadGenerator getEventGenerator()
    {
        return m_eventGenerator;
    }

    private BeansRetriever< HttpNotificationRegistration< ? > > extractNonTransactionNotificationRegistrationService(
            final BeansRetriever< HttpNotificationRegistration< ? > > service )
    {
        try
        {
            final Method mGetServiceManager =
                    BaseBeansRetriever.class.getDeclaredMethod( "getServiceManager" );
            mGetServiceManager.setAccessible( true );
            final BeansServiceManager serviceManager = (BeansServiceManager)mGetServiceManager.invoke( service );
            if ( !serviceManager.isTransaction() )
            {
                return service;
            }
            return serviceManager.getTransactionSource().getRetriever( service.getServicedType() );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException(
                    "Cannot extract non-transaction notification registration service for: "
                            + service.getClass().getName(), ex );
        }
    }
    
    protected final BeansRetriever< HttpNotificationRegistration< ? > > m_retriever;
    private final NotificationPayloadGenerator m_eventGenerator;
}
