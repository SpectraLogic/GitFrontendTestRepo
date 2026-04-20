/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.comparator.NullHandlingComparator;

public final class BeanComparator< B > implements Comparator< B >
{
    public BeanComparator( final Class< B > clazz, final String beanProperty )
    {
        this( clazz, new BeanPropertyComparisonSpecifiction( beanProperty, Direction.ASCENDING, null ) );
    }
    
    
    public BeanComparator( 
            final Class< B > clazz,
            final BeanPropertyComparisonSpecifiction ... comparisonSpecifications )
    {
        for ( final BeanPropertyComparisonSpecifiction spec : comparisonSpecifications )
        {
            m_specs.add( spec.init( clazz ) );
        }
    }
    
    
    public final static class BeanPropertyComparisonSpecifiction
    {
        public BeanPropertyComparisonSpecifiction(
                final String beanProperty,
                final Direction direction,
                final Comparator< Object > comparator )
        {
            Validations.verifyNotNull( "Bean property", beanProperty );
            Validations.verifyNotNull( "Sort direction", direction );
            m_beanProperty = beanProperty;
            m_direction = direction;
            m_comparator = comparator;
        }
        
        private BeanPropertyComparisonSpecifiction init( final Class< ? > clazz )
        {
            m_reader = BeanUtils.getReader( clazz, m_beanProperty );
            m_comparator = new NullHandlingComparator( m_comparator );
            return this;
        }
        
        private Method m_reader;
        private final String m_beanProperty;
        private final Direction m_direction;
        private Comparator< Object > m_comparator;
    } // end inner class def
    
    
    public int compare( final B bean1, final B bean2 )
    {
        return compareInternal( bean1, bean2, 0 );
    }
    
    
    public int compareInternal( final B bean1, final B bean2, final int specIndex )
    {
        final BeanPropertyComparisonSpecifiction spec = m_specs.get( specIndex );
        final int retval = getCompareValue( bean1, bean2, spec );
        if ( 0 == retval && specIndex < m_specs.size() - 1 )
        {
            return compareInternal( bean1, bean2, specIndex + 1 );
        }
        return retval;
    }
    
    
    public int getCompareValue( final B bean1, final B bean2, final BeanPropertyComparisonSpecifiction spec )
    {
        try
        {
            final Object o1 = spec.m_reader.invoke( bean1 );
            final Object o2 = spec.m_reader.invoke( bean2 );
            return spec.m_comparator.compare( o1, o2 ) * spec.m_direction.getComparisonMultiplier();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( 
                    "Failed to compare " + bean1 + " and " + bean2 + " using " + spec.m_reader + ".", ex );
        }
    }
    
    
    private final List< BeanPropertyComparisonSpecifiction > m_specs = new ArrayList<>();
}
