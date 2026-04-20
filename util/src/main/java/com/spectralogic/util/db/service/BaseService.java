/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;

public abstract class BaseService< T extends DatabasePersistable > extends BaseDatabaseBeansRetriever< T >
{
    protected BaseService( final Class< T > clazz )
    {
        super( clazz, GenericFailure.NOT_FOUND );
    }
    
    
    protected BaseService( final Class< T > clazz, final FailureType notFoundFailure )
    {
        super( clazz, notFoundFailure );
    }
    
    
    public void delete( final UUID id )
    {
        getDataManager().deleteBean( getServicedType(), id );
    }
    
    
    public void delete( final Set< UUID > ids )
    {
        getDataManager().deleteBeans(
                getServicedType(),
                Require.beanPropertyEqualsOneOf( Identifiable.ID, ids ) );
    }
    
    
    public void delete( final WhereClause filter )
    {
        getDataManager().deleteBeans( getServicedType(), filter );
    }
    
    
    public void deleteAll()
    {
        getDataManager().deleteBeans( getServicedType(), Require.nothing() );
    }
    
    
    public void create( final T bean )
    {
        getDataManager().createBean( bean );
    }
    
    
    public void create( final Set< T > beans )
    {
        getDataManager().createBeans( beans );
    }
    
    
    public void update( final T bean, final String... propertiesToUpdate )
    {
        if ( null == propertiesToUpdate || propertiesToUpdate.length == 0 )
        {
            LOG.info( "No properties to update were specified for " + bean + "." );
            return;
        }
        getDataManager().updateBean( CollectionFactory.toSet( propertiesToUpdate ), bean );
    }


    public void update(final WhereClause whereClause, final Consumer<T> beanConsumer, final String... propertiesToUpdate)
    {
        final T bean = BeanFactory.newBean( getServicedType() );
        beanConsumer.accept(bean);
        getDataManager().updateBeans(CollectionFactory.toSet(propertiesToUpdate), bean, whereClause);
    }
    
    
    final protected void verifyInsideTransaction()
    {
        if ( isTransaction() )
        {
            return;
        }
        
        throw new IllegalStateException( 
                "The requested operation must be performed within a transaction." );
    }
    
    
    final protected void verifyNotTransaction()
    {
        if ( !isTransaction() )
        {
            return;
        }
        
        throw new IllegalStateException( 
                "The requested operation cannot be performed within a transaction." );
    }
    
    
    final protected boolean isTransaction()
    {
        return getServiceManager().isTransaction();
    }
    
    
    /**
     * @param beans - remove the existent persisted beans from the input Set of beans
     * @return the number of removed beans
     */
    final protected int removeExistentPersistedBeansFromSet( final Set< T > beans )
    {
        Validations.verifyNotNull( "Beans", beans );
        final int originalCount = beans.size();
        final Set< WhereClause > filters = new HashSet<>();
        final Set< T > segment = new HashSet<>();
        for ( final T bean : new HashSet<>( beans ) )
        {
            segment.add( bean );
            final Set< WhereClause > propChecks = new HashSet<>();
            for ( final String prop : BeanUtils.getPropertyNames( getServicedType() ) )
            {
                if ( isExemptFromEqualityChecker( getServicedType(), prop ) )
                {
                    continue;
                }
                
                try
                {
                    propChecks.add( Require.beanPropertyEquals(
                            prop,
                            BeanUtils.getReader( getServicedType(), prop ).invoke( bean ) ) );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( "Failed to read prop " + prop + ".", ex );
                }
            }
            filters.add( Require.all( propChecks ) );
            if ( 500 <= filters.size() )
            {
                beans.removeAll( getExistentPersistedBeans( filters, segment ) );
                filters.clear();
                segment.clear();
            }
        }
        
        beans.removeAll( getExistentPersistedBeans( filters, segment ) );
        
        return originalCount - beans.size();
    }
    
    
    private Set< T > getExistentPersistedBeans( 
            final Set< WhereClause > filters,
            final Set< T > beans )
    {
        Validations.verifyNotNull( "Beans", beans );
        Validations.verifyNotNull( "Filters", filters );
        if ( beans.isEmpty() )
        {
            return new HashSet<>();
        }
        
        final Map< BeanPropertyValues< T >, T > beanValues = new HashMap<>();
        for ( final T bean : beans )
        {
            beanValues.put( new BeanPropertyValues<>( getServicedType(), bean ), bean );
        }
        
        final Set< T > retval = new HashSet<>();
        final WhereClause requireAnyWhereClause = Require.any( filters );
        final RetrieveBeansResult< T > getAllBeans = retrieveAll( requireAnyWhereClause );
        for ( final T existing : getAllBeans.toSet() )
        {
            retval.add( beanValues.get( new BeanPropertyValues<>( getServicedType(), existing ) ) );
        }
        
        return retval;
    }
    
    
    private final static class BeanPropertyValues< T >
    {
        private BeanPropertyValues( final Class< T > clazz, final T bean )
        {
            for ( final String prop : BeanUtils.getPropertyNames( clazz ) )
            {
                if ( isExemptFromEqualityChecker( clazz, prop ) )
                {
                    continue;
                }
                try
                {
                    m_propValues.put( prop, BeanUtils.getReader( clazz, prop ).invoke( bean ) );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( "Failed to read prop " + prop + ".", ex );
                }
            }
        }
        
        
        @Override
        public int hashCode()
        {
            return m_propValues.hashCode();
        }
        
        
        @Override
        public boolean equals( final Object uncastedOther )
        {
            if ( this == uncastedOther )
            {
                return true;
            }
            if ( null == uncastedOther )
            {
                return false;
            }
            if ( ! ( uncastedOther instanceof BeanPropertyValues ) )
            {
                return false;
            }
            
            final BeanPropertyValues< ? > other = (BeanPropertyValues< ? >)uncastedOther;
            return m_propValues.equals( other.m_propValues );
        }
        
        
        private final Map< String, Object > m_propValues = new HashMap<>();
    } // end inner class def
    
    
    private static boolean isExemptFromEqualityChecker( final Class< ? > clazz, final String prop )
    {
        return ( Identifiable.ID.equals( prop )
                || BeanUtils.getReader( clazz, prop ).getReturnType() == Date.class 
                || null != BeanUtils.getReader( clazz, prop ).getAnnotation( Secret.class ) );
    }
}
