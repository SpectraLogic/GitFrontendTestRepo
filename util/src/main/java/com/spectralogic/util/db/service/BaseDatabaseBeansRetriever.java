/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.manager.frmwrk.SqlStatementExecutor;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Query.Retrievable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.lang.iterate.PreProcessor;

public class BaseDatabaseBeansRetriever< T extends DatabasePersistable > extends BaseBeansRetriever< T >
{
    protected BaseDatabaseBeansRetriever( final Class< T > clazz, final FailureType notFoundFailure )
    {
        super( clazz, notFoundFailure );
    }
    
    
    public T discover( final Object someBeanPropertyValue )
    {
        return getDataManager().discover( getServicedType(), someBeanPropertyValue );
    }
    
    
    final public int getCount()
    {
        return getCount( Require.nothing() );
    }
    
    
    public int getCount( final Retrievable retrievable )
    {
        return getDataManager().getCount( getServicedType(), retrievable );
    }
    
    
    public int getCount( final WhereClause whereClause )
    {
        return getDataManager().getCount( getServicedType(), whereClause );
    }

    
    public long getMin( final String propertyName, final WhereClause whereClause )
    {
        return getDataManager().getMin( getServicedType(), propertyName, whereClause );
    }


    public long getMax( final String propertyName, final WhereClause whereClause )
    {
        return getDataManager().getMax( getServicedType(), propertyName, whereClause );
    }


    public long getSum( final String propertyName, final WhereClause whereClause )
    {
        return getDataManager().getSum( getServicedType(), propertyName, whereClause );
    }


    @Override
    final protected T findSingleResult(
            final NotFoundBehavior notFoundBehavior, 
            final WhereClause whereClause )
    {
        final EnhancedIterable< T > results =
                getDataManager().getBeans( getServicedType(), Query.where( whereClause ) );
        populate( results );
        final Set< T > beans = results.toSet();
                
        if ( 1 == beans.size() )
        {
            return beans.iterator().next();
        }
        
        switch ( notFoundBehavior )
        {
            case RETURN_NULL:
                return null;
            case THROW_EXCEPTION:
                final String where = SqlStatementExecutor.getReadableSql( getServicedType(), whereClause );
                verifySingleResult(
                        getServicedType().getSimpleName() + " does not exist where " + where,
                        getServicedType().getSimpleName() + " returns more than one result where " + where,
                        beans );
                throw new RuntimeException( "Should be impossible to hit this line of code." );
            default:
                throw new UnsupportedOperationException( "No code to support " + notFoundBehavior );
        }
    }


    @Override
    public EnhancedIterable< T > retrieveIterable( final Retrievable retrievable )
    {
        return getDataManager().getBeans( getServicedType(), retrievable );
    }
    
    
    @Override
    final protected void populate( final EnhancedIterable< T > results )
    {
        for ( final PreProcessor< T > pp : m_customPopulators )
        {
            results.register( pp );
        }
    }
    
    
    final protected void addCustomBeanPopulationProcessor( final PreProcessor< T > customPopulator )
    {
        m_customPopulators.add( customPopulator );
    }
    
    
    private final List< PreProcessor< T > > m_customPopulators = new CopyOnWriteArrayList<>();
}
