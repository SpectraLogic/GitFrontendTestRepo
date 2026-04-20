/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.List;
import java.util.Objects;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.lang.Validations;

final class ExistsWhereClause implements WhereClause
{
    ExistsWhereClause( final String propertyName, final WhereClause nestedTypeWhereClause )
    {
        m_propertyName = propertyName;
        m_nestedType = null; //must populate during toSql call, we don't know it here due to type erasure
        m_nestedPropertyName = "id";
        m_nestedTypeWhereClause = nestedTypeWhereClause;
        Validations.verifyNotNull( "Bean property", m_propertyName);
        Validations.verifyNotNull( "Referenced type where clause", m_nestedTypeWhereClause );
    }
    
    
    ExistsWhereClause(
            final Class< ? extends DatabasePersistable > nestedType,
            final String nestedTypePropertyName,
            final WhereClause nestedTypeWhereClause )
    {
        m_propertyName = "id";
        m_nestedType = nestedType;
        m_nestedPropertyName = nestedTypePropertyName;
        m_nestedTypeWhereClause = nestedTypeWhereClause;
        Validations.verifyNotNull( "Nested type", m_nestedType );
        Validations.verifyNotNull( "Nested bean property", m_nestedPropertyName);
        Validations.verifyNotNull( "Referenced type where clause", m_nestedTypeWhereClause );
    }


    ExistsWhereClause(
            final String propertyName,
            final Class< ? extends DatabasePersistable > nestedType,
            final String nestedTypePropertyName,
            final WhereClause nestedTypeWhereClause )
    {
        m_propertyName = propertyName;
        m_nestedType = nestedType;
        m_nestedPropertyName = nestedTypePropertyName;
        m_nestedTypeWhereClause = nestedTypeWhereClause;
        Validations.verifyNotNull( "Current type bean property", m_nestedPropertyName);
        Validations.verifyNotNull( "Nested type", m_nestedType );
        Validations.verifyNotNull( "Nested type bean property", m_nestedPropertyName);
        Validations.verifyNotNull( "Referenced type where clause", m_nestedTypeWhereClause );
    }

    
    public String toSql( 
            final Class< ? extends DatabasePersistable > clazz, 
            final List< Object > sqlParameters )
    {
        if (m_nestedType == null) {
            final References references =
                    BeanUtils.getReader( clazz, m_propertyName).getAnnotation( References.class );
            if ( null == references ) {
                throw new RuntimeException(
                        "Bean property " + clazz.getName() + "." + m_nestedPropertyName
                                + " does not reference another type." );
            } else {
                m_nestedType = references.value();
            }
        }
        if (Objects.equals(m_propertyName, "id")) {
            final References references =
                    BeanUtils.getReader( m_nestedType, m_nestedPropertyName).getAnnotation( References.class );
            if ( null == references )
            {
                throw new RuntimeException(
                        "Bean property " + m_nestedType.getName() + "." + m_nestedPropertyName
                                + " does not reference another type." );
            }
            if ( !references.value().isAssignableFrom( clazz ) )
            {
                throw new RuntimeException(
                        "Bean property " + m_nestedType.getName() + "." + m_nestedPropertyName + " refers to "
                                + references.value() + ", but should have referred to " + clazz + "." );
            }
        }

        return "EXISTS (SELECT * FROM "
                + DatabaseNamingConvention.toDatabaseTableName(m_nestedType)
                + " WHERE " + DatabaseNamingConvention.toDatabaseTableName(clazz) + "."
                + DatabaseNamingConvention.toDatabaseColumnName(m_propertyName) + " = "
                + DatabaseNamingConvention.toDatabaseTableName(m_nestedType) + "."
                + DatabaseNamingConvention.toDatabaseColumnName(m_nestedPropertyName) + " AND ("
                + m_nestedTypeWhereClause.toSql(m_nestedType, sqlParameters)
                + "))";
    }
    

    private Class< ? extends DatabasePersistable > m_nestedType;
    private final String m_propertyName;
    private final String m_nestedPropertyName;
    private final WhereClause m_nestedTypeWhereClause;
}
