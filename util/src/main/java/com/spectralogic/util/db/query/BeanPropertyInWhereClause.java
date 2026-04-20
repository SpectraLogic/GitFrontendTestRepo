/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.BeanPropertyWhereClause.ComparisonType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

final class BeanPropertyInWhereClause implements WhereClause
{
    BeanPropertyInWhereClause( final String propertyName, final Collection< ? > values )
    {
        m_propertyName = propertyName;
        m_values = values;
        Validations.verifyNotNull( "Property name", m_propertyName );
        Validations.verifyNotNull( "Values", m_values );
    }
    
    
    public String toSql( 
            final Class< ? extends DatabasePersistable > clazz, 
            final List< Object > sqlParameters )
    {
        final Class< ? > type = propertyType( clazz );
        final String databaseColumnName = DatabaseNamingConvention.toDatabaseColumnName( m_propertyName );
        final Collection< ? > parameters = buildParameters( type );

        // We found a bug in the PostgreSQL JDBC driver where
        // Connection.createArrayOf doesn't handle schema qualified type names
        // properly. Since there probably won't be many values for enums we can
        // get away with continuing to use an IN clause.
        // https://github.com/pgjdbc/pgjdbc/issues/189
        if ( type.isEnum() )
        {
            sqlParameters.addAll( parameters );
            return buildEnumInClause(
                    databaseColumnName,
                    DatabaseNamingConvention.toDatabaseEnumName( type ),
                    m_values.size() );
        }

        sqlParameters.add( toArray( type, parameters ) );
        return databaseColumnName + " = ANY (?)";
    }


    /**
     * This is just like CollectionFactory.toArray except in this case we don't
     * know the type at compile time so we can't use generics.
     */
    private Object[] toArray( final Class< ? > type, final Collection< ? > collection )
    {
        final Object[] retval = (Object[])Array.newInstance( type, collection.size() );
        collection.toArray( retval );
        return retval;
    }


    private static String buildEnumInClause(
            final String databaseColumnName,
            final String dbEnumName,
            final int parameterCount )
    {
        final StringBuilder sql = new StringBuilder();
        sql.append( databaseColumnName ).append( " IN (" );
        for ( int i = 0; i < parameterCount; ++i )
        {
            if ( i > 0 )
            {
                sql.append( ", " );
            }
            sql.append( "CAST(? AS " ).append( dbEnumName ).append( ')' );
        }
        sql.append( ')' );
        return sql.toString();
    }


    private Class< ? > propertyType( final Class< ? extends DatabasePersistable > clazz )
    {
        final Method reader = BeanUtils.getReader( clazz, m_propertyName );
        Validations.verifyNotNull( "Reader for " + clazz + "." + m_propertyName, reader );
        new BeanPropertyWhereClause( ComparisonType.EQUALS, m_propertyName, null )
                .performSecurityValidation( reader );
        return ReflectUtil.toNonPrimitiveType( reader.getReturnType() );
    }


    private Collection< ? > buildParameters( final Class< ? > type )
    {
        final Collection< Object > values = new ArrayList<>();
        for ( final Object v : m_values )
        {
            values.add( new BeanPropertyWhereClause( ComparisonType.EQUALS, m_propertyName, v )
                    .getValue( type ) );
        }
        return values;
    }
    
    
    private final String m_propertyName;
    private final Collection< ? > m_values;
}
