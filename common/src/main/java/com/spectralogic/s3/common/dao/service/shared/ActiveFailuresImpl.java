/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;

public final class ActiveFailuresImpl
    < T extends DatabasePersistable & Failure< T, ? >, S extends FailureService< T > >
    extends BaseShutdownable
    implements ActiveFailures
{
    public ActiveFailuresImpl( final S service, final T baseBean )
    {
        Validations.verifyNotNull( "Service", service );
        Validations.verifyNotNull( "Base bean", baseBean );
        
        m_clazz = service.getServicedType();
        m_baseBean = baseBean;
        final Set< WhereClause > filters = new HashSet<>();
        for ( final String prop : service.getFailureDescribingProps() )
        {
            try
            {
                final Object value = BeanUtils.getReader( m_clazz, prop ).invoke( m_baseBean );
                Validations.verifyNotNull( "Value for property '" + prop + "'", value );
                filters.add( Require.beanPropertyEquals( prop, value ) );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        m_filter = Require.all( filters );
        
        m_service = service;
        
        doNotLogWhenShutdown();
    }


    public ActiveFailuresImpl( final S service, final T baseBean,  int aggregationTime ) {
        this(service, baseBean);
        m_timeUnit = aggregationTime;

    }
    
    public WhereClause getFilter()
    {
        return m_filter;
    }
    
    
    public void add( final Throwable t )
    {
        add( ExceptionUtil.getReadableMessage( t ) );
    }
    
    
    synchronized public void add( final String failure )
    {
        verifyNotShutdown();
        m_failures.add( failure );
    }
    
    
    synchronized public void commit()
    {
        verifyNotShutdown();
        shutdown();
        
        final Map< UUID, String > existingFailures = BeanUtils.toMap( 
                m_service.retrieveAll( m_filter ).toSet(),
                ErrorMessageObservable.ERROR_MESSAGE );
        
        final Set< String > newFailures = new HashSet<>( m_failures );
        newFailures.removeAll( existingFailures.values() );
        
        final Set< String > obsoleteFailures = new HashSet<>( existingFailures.values() );
        obsoleteFailures.removeAll( m_failures );

        for ( final String newFailure : newFailures )
        {
            final T bean = BeanFactory.newBean( m_clazz );
            BeanCopier.copy( bean, m_baseBean );
            bean.setErrorMessage( newFailure );
            bean.setId( UUID.randomUUID() );
            m_service.create( bean, m_timeUnit );
        }
        for ( final Map.Entry< UUID, String > e : existingFailures.entrySet() )
        {
            if ( obsoleteFailures.contains( e.getValue() ) )
            {
                m_service.delete( e.getKey() );
            }
        }
    }
    
    
    private final S m_service;
    private final WhereClause m_filter;
    private final T m_baseBean;
    private final Class< T > m_clazz;
    private final Set< String > m_failures = new HashSet<>();
    private int m_timeUnit;
}
