/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Query.Retrievable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public class BaseAggregateBeansRetriever< T extends SimpleBeanSafeToProxy > extends BaseBeansRetriever< T >
{
    protected BaseAggregateBeansRetriever( final Class< T > clazz )
    {
        this( clazz, GenericFailure.NOT_FOUND );
    }
    
    
    protected BaseAggregateBeansRetriever( final Class< T > clazz, final FailureType notFoundFailure )
    {
        super( clazz, notFoundFailure );
    }
    
    
    public T discover( final Object someBeanPropertyValue )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    public int getCount( final Retrievable retrievable )
    {
        return getCount( Require.nothing() );
    }
    
    
    final public int getCount()
    {
        return getCount( Require.nothing() );
    }


    public int getCount( final WhereClause whereClause )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }

    
    public long getMin( final String propertyName, final WhereClause whereClause )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public long getMax( final String propertyName, final WhereClause whereClause )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public long getSum( final String propertyName, final WhereClause whereClause )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    @Override public EnhancedIterable< T > retrieveIterable( final Retrievable retrievable )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    @Override
    protected T findSingleResult( final NotFoundBehavior notFoundBehavior, final WhereClause whereClause )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
}
