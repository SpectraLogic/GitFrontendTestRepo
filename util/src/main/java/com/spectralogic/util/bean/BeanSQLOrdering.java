/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.lang.Validations;

/**
 * Manages a beans column sort ordering to enable building SQL for indicated sorting.
 */
public final class BeanSQLOrdering
{
    public BeanSQLOrdering()
    {
    }
    
    
    BeanSQLOrdering( final BeanSQLOrderingSpecification... comparisonSpecifications )
    {
        m_specs.addAll( Arrays.asList( comparisonSpecifications ) );
    }
    
    
    public void add( final String property, final Direction direction )
    {
        m_specs.add( new BeanSQLOrderingSpecification( property, direction ) );
    }
    
    
    public boolean hasColumn( String column )
    {
        return m_specs.stream()
                      .anyMatch( x -> x.property.equals( column ) );
    }
    
    
    public final static class BeanSQLOrderingSpecification
    {
    
        public BeanSQLOrderingSpecification( final String property, final Direction direction )
        {
            Validations.verifyNotNull( "Sort property", property );
            Validations.verifyNotNull( "Sort direction", direction );
            this.property = property;
            this.direction = direction;
        }
        
        
        public String getColumnName()
        {
            return property;
        }
        
        
        /*
         * Note: The property has to be stored in non-database format for checking of dynamic sort fields.
         * And, the direction cannot be filtered through the the column name filter.
         */
        public String toSql()
        {
            return DatabaseNamingConvention.toDatabaseColumnName( property ) + " " + direction.getSqlDirection();
        }
        
        
        private final String property;
        private final Direction direction;
    }
    
    
    public List< BeanSQLOrderingSpecification > getSpecs()
    {
        return m_specs;
    }
    
    
    public List< String > getSortColumnNames()
    {
        return m_specs.stream()
                      .map( BeanSQLOrderingSpecification::getColumnName )
                      .collect( Collectors.toList() );
    }
    
    
    private final List< BeanSQLOrderingSpecification > m_specs = new ArrayList<>();
}
