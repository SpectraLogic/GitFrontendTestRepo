/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.log.LogUtil;

final class BeanPropertyWhereClause implements WhereClause
{
    enum ComparisonType
    {
        EQUALS( "=" ),
        MATCHES_INSENSITIVE( "ILIKE" ),
        MATCHES( "LIKE" ),
        GREATER_THAN( ">" ),
        LESS_THAN( "<" ),
        ;
        
        
        private ComparisonType( final String operator )
        {
            m_operator = operator;
        }
        
        
        private String getOperator()
        {
            return m_operator;
        }
        
        
        private final String m_operator;
    }
    
    
    enum MultipleBeanPropertiesAggregationMode
    {
        SUM( "+" ),
        ;
        
        
        private MultipleBeanPropertiesAggregationMode( final String operator )
        {
            m_operator = operator;
        }
        
        
        private String getOperator()
        {
            return m_operator;
        }
        
        
        private final String m_operator;
    }
    
    
    BeanPropertyWhereClause(
            final ComparisonType comparisonType,
            final String propertyName, 
            final Object value )
    {
        this( comparisonType,
              MultipleBeanPropertiesAggregationMode.values()[ 0 ],
              new String [] { propertyName }, 
              value );
    }
    
    
    BeanPropertyWhereClause(
            final ComparisonType comparisonType,
            final MultipleBeanPropertiesAggregationMode multipleBeanPropertiesAggregationMode,
            final String[] propertyNames,
            final Object value )
    {
        m_comparisonType = comparisonType;
        m_multipleBeanPropertiesAggregationMode = multipleBeanPropertiesAggregationMode;
        m_propertyNames = ( null == propertyNames ) ? null : propertyNames.clone();
        m_value = value;
    
        if ( ( ( ComparisonType.MATCHES_INSENSITIVE == m_comparisonType ) ||
                ( ComparisonType.MATCHES == m_comparisonType ) ) && ( null != m_value ) &&
                ( String.class != m_value.getClass() ) )
        {
            throw new UnsupportedOperationException( "Must include a string type." );
        }
        
        Validations.verifyNotNull( "Comparison type", m_comparisonType );
        Validations.verifyNotNull( "Aggregation mode", m_multipleBeanPropertiesAggregationMode );
        Validations.verifyNotNull( "Property names", m_propertyNames );
        Validations.verifyInRange( "Number of property names", 1, 16, m_propertyNames.length );
        for ( final String propertyName : m_propertyNames )
        {
            Validations.verifyNotNull( "Property name", propertyName );
        }
    }
    
    
    public String toSql(
            final Class< ? extends DatabasePersistable > clazz,
            final List< Object > sqlParameters )
    {
        final Set< Class< ? > > types = new HashSet<>();
        String fullColumnSpec = "";
        for ( final String propertyName : m_propertyNames )
        {
            final Method reader = BeanUtils.getReader( clazz, propertyName );
            Validations.verifyNotNull( "Reader for " + clazz + "." + propertyName, reader );
            performSecurityValidation( reader );
            types.add( ReflectUtil.toNonPrimitiveType( reader.getReturnType() ) );
            
            final String columnName = DatabaseNamingConvention.toDatabaseColumnName( propertyName );
            if ( 0 != fullColumnSpec.length() )
            {
                fullColumnSpec += " " + m_multipleBeanPropertiesAggregationMode.getOperator() + " ";
            }
            fullColumnSpec += columnName;
        }
        
        if ( 1 != types.size() )
        {
            throw new UnsupportedOperationException( 
                    "Bean properties " + CollectionFactory.toList( m_propertyNames ) 
                    + " have different types: " + types );
        }
        final Class< ? > type = types.iterator().next();
        
        String sqlValuePlaceholder = "?";
        final Object sqlValue = getValue( type );
        if ( null == sqlValue )
        {
            if ( ComparisonType.EQUALS != m_comparisonType )
            {
                throw new UnsupportedOperationException(
                        "Cannot perform " + m_comparisonType + " comparison against null." );
            }
            return fullColumnSpec + " IS NULL";
        }
        
        if ( types.iterator().next().isEnum() )
        {
            final String dbEnumName = DatabaseNamingConvention.toDatabaseEnumName( type );
            sqlValuePlaceholder = "CAST(? AS " + dbEnumName + ")";
        }
        
        sqlParameters.add( sqlValue );
        return fullColumnSpec + " " + m_comparisonType.getOperator() + " " + sqlValuePlaceholder;
    }
    
    
    Object getValue( Class< ? > expectedType )
    {
        if ( null == m_value )
        {
            return null;
        }
        
        Object value = m_value;
        if ( DynamicValueProvider.class.isAssignableFrom( value.getClass() ) )
        {
            value = ( (DynamicValueProvider)value ).getValue();
        }
        if ( null == value )
        {
            return null;
        }
                
        expectedType = ReflectUtil.toNonPrimitiveType( expectedType );
        if ( expectedType.isEnum() )
        {
            return ReflectUtil.enumValueOf( expectedType, value.toString() );
        }
        if ( UUID.class == expectedType && UUID.class != value.getClass() )
        {
            return UUID.fromString( value.toString() );
        }
        if ( Date.class == expectedType && Date.class != value.getClass() )
        {
            @SuppressWarnings( "deprecation" )
            final Date retval = new Date( value.toString() );
            return retval;
        }
        if ( Long.class == expectedType && String.class == value.getClass() )
        {
            return Long.valueOf( Long.parseLong( value.toString() ) );
        }
        if ( Integer.class == expectedType && String.class == value.getClass() )
        {
            return Integer.valueOf( Integer.parseInt( value.toString() ) );
        }
        if (Boolean.class == expectedType && String.class == value.getClass())
        {
            if (((String) value).equalsIgnoreCase("true") ||
                    ((String) value).equalsIgnoreCase("false")) {
                return Boolean.valueOf(value.toString());
            } else {
                throw new IllegalArgumentException("Invalid boolean string: " + value);
            }
        }
        
        return value;
    }
    
    
    @Override
    public String toString()
    {
        return ( ( 1 == m_propertyNames.length ) ?
                m_propertyNames[ 0 ] 
                : CollectionFactory.toList( m_propertyNames ) ) 
               + " " + m_comparisonType.getOperator() + " " + m_value;
    }
    
    
    void performSecurityValidation( final Method reader )
    {
        if ( null != reader.getAnnotation( Secret.class ) )
        {
            final String msg = 
                    "Security Attack Detected: Attempt was made to query by secret data on '" + reader + "'.";
            LOG.warn( LogUtil.getLogMessageHeaderBlock( msg ) );
            throw new DaoException( GenericFailure.FORBIDDEN, msg );
        }
    }
    

    private final ComparisonType m_comparisonType;
    private final MultipleBeanPropertiesAggregationMode m_multipleBeanPropertiesAggregationMode;
    private final String [] m_propertyNames;
    private final Object m_value;
    
    private final static Logger LOG = Logger.getLogger( BeanPropertyWhereClause.class );
}
