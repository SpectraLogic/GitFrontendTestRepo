/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SortByIndexNotNeeded;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;

public final class Query
{
    private Query()
    {
        //empty
    }
    
    
    public static OrderableRetrievable where( final WhereClause whereClause )
    {
        return new FilteredRetrievable( whereClause );
    }
    
    public interface OrderableRetrievable extends Retrievable
    {
        LimitableRetrievable orderBy( final BeanSQLOrdering beanSQLOrdering );
    
        LimitableRetrievable orderByNone();
    }//end inner class
    
    
    public interface LimitableRetrievable extends OffsettableRetrievable
    {
        OffsettableRetrievable limit( final int limit );
    }//end inner class
    
    
    public interface OffsettableRetrievable extends Retrievable
    {
        Retrievable offset( final int offset );
    }//end inner class


    public interface Retrievable
    {
        /**
         * @param sqlParameters - parameters corresponding to ? entries in the SQL returned
         * @return the TSQL to execute
         */
        String toSql(
                final Class< ? extends DatabasePersistable > clazz, 
                final List< Object > sqlParameters );
    }//end inner class
    
    
    private static class FilteredRetrievable implements OrderableRetrievable
    {
        FilteredRetrievable( final WhereClause whereClause )
        {
            m_whereClause = whereClause;
        }
    
    
        public LimitableRetrievable orderBy( final BeanSQLOrdering beanSQLOrdering )
        {
            return new OrderedRetrievable( this, beanSQLOrdering );
        }
    
    
        public LimitableRetrievable orderByNone()
        {
            return new OrderedRetrievable( this );
        }


        public String toSql(
                final Class< ? extends DatabasePersistable > clazz,
                final List< Object > sqlParameters )
        {
            return new StringBuilder()
                    .append( "SELECT * FROM " )
                    .append( DatabaseNamingConvention.toDatabaseTableName( clazz ) )
                    .append( " WHERE " )
                    .append( m_whereClause.toSql( clazz, sqlParameters ) )
                    .toString();
        }
        
        
        private final WhereClause m_whereClause;
    }//end inner class
    
    
    private static class OrderedRetrievable implements LimitableRetrievable
    {
    
        OrderedRetrievable( final OrderableRetrievable orderableRetrievable, final BeanSQLOrdering beanSQLOrdering )
        {
            m_orderableRetrievable = orderableRetrievable;
            m_beanSQLOrdering = beanSQLOrdering;
            m_sort = true;
        }
    
    
        OrderedRetrievable( final OrderableRetrievable orderableRetrievable )
        {
            m_orderableRetrievable = orderableRetrievable;
            m_sort = false;
        }
        
        
        public OffsettableRetrievable limit( final int limit )
        {
            return new LimitedRetrievable( this, limit );
        }
        
        
        public Retrievable offset( final int offset )
        {
            return new OffsetRetrievable( this, offset );
        }


        public String toSql(
                final Class< ? extends DatabasePersistable > clazz,
                final List< Object > sqlParameters )
        {
            validateOrderBy( clazz );
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append( m_orderableRetrievable.toSql( clazz, sqlParameters ) );
    
            if ( m_sort )
            {
                List< String > sortColumns = m_beanSQLOrdering.getSpecs()
                                                              .stream()
                                                              .map( x -> x.toSql() )
                                                              .collect( Collectors.toList() );
                sortColumns.addAll( removeDuplicateSortColumns( clazz ) );
    
                boolean isFirst = true;
                for ( final String name : sortColumns )
                {
                    if ( isFirst )
                    {
                        isFirst = false;
                        stringBuilder.append( " ORDER BY " );
                    }
                    else
                    {
                        stringBuilder.append( ", " );
                    }
                    stringBuilder.append( name );
                }
            }
            return stringBuilder.toString();
        }
    
    
        private List< String > removeDuplicateSortColumns( final Class< ? extends DatabasePersistable > clazz )
        {
            return BeanUtils.getSqlOrdering( clazz )
                            .getSpecs()
                            .stream()
                            .filter( x -> !m_beanSQLOrdering.hasColumn( x.getColumnName() ) )
                            .map( x -> x.toSql() )
                            .collect( Collectors.toList() );
        }
    
    
        private void validateOrderBy( final Class< ? > type )
        {
            Set< String > indexes = BeanUtils.getColumnIndexes( type );
    
            final List< String > sortProperties = m_beanSQLOrdering.getSortColumnNames();
            sortProperties.addAll( BeanUtils.getSqlOrdering( type )
                                            .getSortColumnNames() );
    
            if ( !indexes.containsAll( sortProperties ) )
            {
                final Method reader = BeanUtils.getReader( type, sortProperties.iterator().next() );
                if ( null == reader )
                {
                    throw new IllegalArgumentException( 
                            "Property " + type.getSimpleName() + "." + sortProperties.iterator().next()
                            + " does not exist." );
                }
                if ( null != type.getAnnotation( SortByIndexNotNeeded.class ) )
                {
                    // Don't need to match indexes to sort by columns
                    return;
                }
                if ( 1 == sortProperties.size() && null != reader.getAnnotation( References.class ) )
                {
                    // There is an implicit index since it referencs another table, so this is fine
                    return;
                }
                throw new UnsupportedOperationException(
                        "Tried to sort by " + m_beanSQLOrdering.getSortColumnNames() + " in " + type.getName() +
                                " but there is no column index with those propertie(s) in them." );
            }
        }
    
    
        private final boolean m_sort;
        private final OrderableRetrievable m_orderableRetrievable;
        private BeanSQLOrdering m_beanSQLOrdering = new BeanSQLOrdering();
    }//end inner class
    
    
    private static class LimitedRetrievable implements OffsettableRetrievable
    {
        LimitedRetrievable( final LimitableRetrievable limitableRetrievable, final int limit )
        {
            m_limitableRetrievable = limitableRetrievable;
            m_limit = limit;
        }


        public Retrievable offset( int offset )
        {
            return new OffsetRetrievable( this, offset );
        }


        public String toSql(
                
                final Class< ? extends DatabasePersistable > clazz,
                final List< Object > sqlParameters )
        {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append( m_limitableRetrievable.toSql( clazz, sqlParameters ) );
            stringBuilder.append( " LIMIT ?" );
            sqlParameters.add( Integer.valueOf( m_limit ) );
            return stringBuilder.toString();
        }
        
        
        private final LimitableRetrievable m_limitableRetrievable;
        private final int m_limit;
    }//end inner class
    
    
    private static class OffsetRetrievable implements Retrievable
    {
        private final OffsettableRetrievable m_offsettableRetrievable;
        private final int m_offset;

        OffsetRetrievable( final OffsettableRetrievable offsettableRetrievable, final int offset )
        {
            m_offsettableRetrievable = offsettableRetrievable;
            m_offset = offset;
        }

        public String toSql(
                final Class< ? extends DatabasePersistable > clazz,
                final List< Object > sqlParameters )
        {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append( m_offsettableRetrievable.toSql( clazz, sqlParameters ) );
            stringBuilder.append( " OFFSET ?" );
            sqlParameters.add( Integer.valueOf( m_offset ) );
            return stringBuilder.toString();
        }
    }//end inner class
}
