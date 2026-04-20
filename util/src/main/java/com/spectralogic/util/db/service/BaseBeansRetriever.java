/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Query.Retrievable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverInitializer;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;

public abstract class BaseBeansRetriever< T extends SimpleBeanSafeToProxy > implements BeansRetriever< T >
{
    protected BaseBeansRetriever( final Class< T > clazz, final FailureType notFoundFailure )
    {
        m_clazz = clazz;
        m_notFoundFailure = notFoundFailure;
        Validations.verifyNotNull( "Class", m_clazz );
        Validations.verifyNotNull( "Not found failure", m_notFoundFailure );
    }
    
    
    protected final void addInitializer( final BeansRetrieverInitializer initializer )
    {
        Validations.verifyNotNull( "Initializer", initializer );
        if ( null != m_bsm )
        {
            throw new IllegalStateException( "Cannot add initializers after initialization has occurred." );
        }
        
        m_initializers.add( initializer );
    }
    
    
    public final void setInitParams(
            final BeansServiceManager bsm, 
            final DataManager dataManager,
            final NotificationEventDispatcher notificationEventDispatcher )
    {
        Validations.verifyNotNull( "Beans service manager", bsm );
        Validations.verifyNotNull( "Data manager", dataManager );
        Validations.verifyNotNull( "Notification event dispatcher", notificationEventDispatcher );
        if ( null != m_bsm )
        {
            throw new IllegalStateException( "Beans retriever manager already set." );
        }
        if ( null != m_dataManager )
        {
            throw new IllegalStateException( "Data manager already set." );
        }
        if ( null != m_notificationEventDispatcher )
        {
            throw new IllegalStateException( "Notification event dispatcher already set." );
        }
        
        m_bsm = bsm;
        m_dataManager = dataManager;
        m_notificationEventDispatcher = notificationEventDispatcher;
    }
    
    
    final public void initialize()
    {
        if ( m_initialized.getAndSet( true ) )
        {
            throw new IllegalStateException( "Already initialized." );
        }
        if ( null == m_bsm )
        {
            throw new IllegalStateException( "Init params must be set first." );
        }
        
        for ( final BeansRetrieverInitializer initializer : m_initializers )
        {
            initializer.initialize();
        }
    }
    
    
    protected final DataManager getDataManager()
    {
        return m_dataManager;
    }
    
    
    protected final BeansServiceManager getServiceManager()
    {
        return m_bsm;
    }
    
    
    protected final NotificationEventDispatcher getNotificationEventDispatcher()
    {
        return m_notificationEventDispatcher;
    }
    
    
    public final Class< T > getServicedType()
    {
        return m_clazz;
    }
    
    
    final public int getCount(
            final String propertyName,
            final Object valueForPropertyName )
    {
        return getCount( Require.beanPropertyEquals( propertyName, valueForPropertyName ) );
    }
    
    
    final protected void verifySingleResult( final String notFoundMessage, final String multipleFoundMessage, final Collection< ? > results )
    {
        if ( null == results || results.isEmpty() )
        {
            throw new DaoException( 
                    m_notFoundFailure, 
                    notFoundMessage );
        }
        if ( 1 < results.size() )
        {
            throw new DaoException(
                    GenericFailure.MULTIPLE_RESULTS_FOUND,
                    multipleFoundMessage );
        }
    }
    
    
    public T attain( final UUID id )
    {
        Validations.verifyNotNull( "ID", id );
        return findSingleResult(
                NotFoundBehavior.THROW_EXCEPTION, 
                Require.beanPropertyEquals( Identifiable.ID, id ) );
    }
    
    
    final public T attain(
            final String propertyName,
            final Object valueForPropertyName )
    {
        return findSingleResult(
                NotFoundBehavior.THROW_EXCEPTION, 
                Require.beanPropertyEquals( propertyName, valueForPropertyName ) );
    }
    
    
    final public T attain( final WhereClause whereClause )
    {
        return findSingleResult(
                NotFoundBehavior.THROW_EXCEPTION, 
                whereClause );
    }
    
    
    public T retrieve( final UUID id )
    {
        Validations.verifyNotNull( "ID", id );
        return findSingleResult(
                NotFoundBehavior.RETURN_NULL, 
                Require.beanPropertyEquals( Identifiable.ID, id ) );
    }
    
    
    final public T retrieve(
            final String propertyName,
            final Object valueForPropertyName )
    {
        return findSingleResult(
                NotFoundBehavior.RETURN_NULL, 
                Require.beanPropertyEquals( propertyName, valueForPropertyName ) );
    }
    
    
    final public T retrieve( final WhereClause whereClause )
    {
        return findSingleResult(
                NotFoundBehavior.RETURN_NULL, 
                whereClause );
    }
    
    
    protected abstract T findSingleResult(
            final NotFoundBehavior notFoundBehavior,
            final WhereClause whereClause );
    
    
    final public RetrieveBeansResult< T > retrieveAll()
    {
        return retrieveAll( Require.nothing() );
    }


    final public T firstOrNull() {
        final RetrieveBeansResult< T > results = retrieveAll( Query.where(Require.nothing()).orderByNone().limit(1) );
        if (results.isEmpty()) return null;
        return results.getFirst();
    }
    
    
    final public RetrieveBeansResult< T > retrieveAll(
            final String beanPropertyName, 
            final Object beanPropertyValue )
    {
        return retrieveAll( Require.beanPropertyEquals( beanPropertyName, beanPropertyValue ) );
    }
    
    
    final public RetrieveBeansResult< T > retrieveAll( final Set< UUID > ids )
    {
        return retrieveAll( Require.beanPropertyEqualsOneOf( Identifiable.ID, ids ) );
    }
    
    
    final public RetrieveBeansResult< T > retrieveAll( final WhereClause whereClause )
    {
        return retrieveAll( Query.where( whereClause ) );
    }
    
    
    final public RetrieveBeansResult< T > retrieveAll( final Retrievable retrievable )
    {
        final EnhancedIterable< T > retval = retrieveIterable( retrievable );
        populate( retval );
        return new RetrieveBeansResultImpl<>( retval );
    }
    
    
    public abstract EnhancedIterable< T > retrieveIterable( final Retrievable retrievable );


    protected void populate( @SuppressWarnings( "unused" ) final EnhancedIterable< T > results )
    {
        // empty by default
    }
    

    protected final FailureType m_notFoundFailure;
    
    private final Set< BeansRetrieverInitializer > m_initializers = new CopyOnWriteArraySet<>();
    private final Class< T > m_clazz;
    private final AtomicBoolean m_initialized = new AtomicBoolean( false );
    private volatile BeansServiceManager m_bsm;
    private volatile DataManager m_dataManager;
    private volatile NotificationEventDispatcher m_notificationEventDispatcher;
    
    protected final static Logger LOG = Logger.getLogger( BaseDatabaseBeansRetriever.class );
}
