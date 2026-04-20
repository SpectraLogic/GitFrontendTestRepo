/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.notification.NotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.platform.notification.generator.GenericDaoNotificationPayloadGenerator;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.notification.domain.bean.HttpNotificationEvent;

public abstract class BaseFailureService< T extends DatabasePersistable & Failure< T, ? > > 
    extends BaseService< T > implements FailureService< T >
{
    @SuppressWarnings( "unchecked" )
    protected BaseFailureService( final Class< T > clazz )
    {
        super( clazz );
        
        m_failureDescribingProps = new HashSet<>( BeanUtils.getPropertyNames( getServicedType() ) );
        m_failureDescribingProps.removeAll( BeanUtils.getPropertyNames( Failure.class ) );
        m_failureDescribingProps.remove( Identifiable.ID );
        m_failureDescribingProps.add( Failure.TYPE );

        try
        {
            m_notificationRegistrationType = (Class< ? extends NotificationRegistrationObservable< ? > >)
                    Class.forName( 
                            NotificationRegistrationObservable.class.getPackage().getName()
                            + "." + clazz.getSimpleName() + "NotificationRegistration" );
            m_notificationPayloadGeneratorType = (Class< ? extends NotificationPayloadGenerator >)
                    Class.forName( 
                            GenericDaoNotificationPayloadGenerator.class.getPackage().getName()
                            + "." + clazz.getSimpleName() + "NotificationPayloadGenerator" );
            m_notificationPayloadGeneratorCon = 
                    m_notificationPayloadGeneratorType.getConstructor( getServicedType() );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    final public Set< String > getFailureDescribingProps()
    {
        return new HashSet<>( m_failureDescribingProps );
    }
    
    
    final public void deleteOldFailures()
    {
        deleteOldFailures( DEFAULT_FAILURE_AGE_TO_BE_OLD_IN_SECS * 1000L );
    }
    
    
    final public void deleteOldFailures( final long failureAgeToBeOldInMillis )
    {
        deleteAll( Require.beanPropertyLessThan(
                Failure.DATE,
                new Date( System.currentTimeMillis() - failureAgeToBeOldInMillis ) ) );
    }
    
    
    final public void create( final T failure, final Integer minMinutesSinceLastFailureOfSameType )
    {
        synchronized ( RECENT_FAILURE_LOCK )
        {
            if ( null != minMinutesSinceLastFailureOfSameType )
            {
                final ActiveFailuresImpl< T, ? > af = new ActiveFailuresImpl<>( this, failure );
                
                if ( 0 < getCount( Require.all( 
                        af.getFilter(),
                        Require.beanPropertyGreaterThan(
                                Failure.DATE, 
                                new Date( System.currentTimeMillis() 
                                          - minMinutesSinceLastFailureOfSameType.intValue() * 60000L ) ) ) ) )
                {
                    LOG.info( "Will not create " + failure + " since there is a recent, similar failure." );
                    return;
                }
            }
            
            create( failure );
        }
    }
    
    
    @Override
    final public void create( final T failure )
    {
        synchronized ( RECENT_FAILURE_LOCK )
        {
            super.create( failure );
            getNotificationEventDispatcher().fire( new HttpNotificationEvent( 
                    getServiceManager().getRetriever( m_notificationRegistrationType ),
                    newNotificationPayloadGenerator( failure ) ) );
        }
    }
    
    
    private NotificationPayloadGenerator newNotificationPayloadGenerator( final T failure )
    {
        try
        {
            return m_notificationPayloadGeneratorCon.newInstance( failure );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    final protected void deleteAll( final WhereClause filter )
    {
        if ( 0 == getCount( filter ) )
        {
            return;
        }
        getDataManager().deleteBeans( getServicedType(), filter );
    }
    

    private final Set< String > m_failureDescribingProps;
    private final Class< ? extends NotificationRegistrationObservable< ? > > m_notificationRegistrationType;
    private final Class< ? extends NotificationPayloadGenerator > m_notificationPayloadGeneratorType;
    private final Constructor< ? extends NotificationPayloadGenerator > m_notificationPayloadGeneratorCon;
    private final static Object RECENT_FAILURE_LOCK = new Object();
}
