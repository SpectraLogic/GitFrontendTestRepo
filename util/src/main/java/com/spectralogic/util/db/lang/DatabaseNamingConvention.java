/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.lang.NamingConvention;
import com.spectralogic.util.lang.Validations;

public final class DatabaseNamingConvention
{
    private DatabaseNamingConvention()
    {
        // singleton
    }
    
    
    public static String toDatabaseTableName( final Class< ? > javaType )
    {
        Validations.verifyNotNull( "Java type", javaType );
        return DATABASE_TABLE_NAME_CACHE.get( BeanFactory.getType( javaType ) );
    }
    
    
    public static String toDatabaseEnumName( final Class< ? > javaType )
    {
        Validations.verifyNotNull( "Java type", javaType );
        return DATABASE_ENUM_NAME_CACHE.get( javaType );
    }
    
    
    public static Class< ? > toJavaType( final Class< ? > definingClass, final String databaseTypeName )
    {
        Validations.verifyNotNull( "Defining class", definingClass );
        Validations.verifyNotNull( "Database type name", databaseTypeName );
        try
        {
            return Class.forName( 
                    definingClass.getName() + "$" + JAVA_TYPE_NAME_CACHE.get( databaseTypeName ) );
        }
        catch ( final ClassNotFoundException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    public static Class< ? > toJavaType( final String typePackage, final String databaseTypeName )
    {
        Validations.verifyNotNull( "Type package", typePackage );
        Validations.verifyNotNull( "Database type name", databaseTypeName );
        try
        {
            return Class.forName( 
                    typePackage + "." + JAVA_TYPE_NAME_CACHE.get( databaseTypeName ) );
        }
        catch ( final ClassNotFoundException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    public static String toDatabaseColumnName( final String beanPropertyName )
    {
        return NamingConvention.toUnderscoredNamingConvention( beanPropertyName );
    }
    
    
    public static String toBeanPropertyName( final String databaseColumnName )
    {
        return NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( databaseColumnName );
    }
    
    
    public static String getSchemaName( final Class< ? > javaType )
    {
        return JAVA_TYPE_SCHEMA_NAME_CACHE.get( javaType );
    }
    
    
    private final static class DatabaseTableNameCacheResultProvider
        implements CacheResultProvider< Class< ? >, String >
    {
        public String generateCacheResultFor( final Class< ? > javaType )
        {
            final String schemaName = getSchemaName( javaType );
            if ( null == schemaName )
            {
                throw new IllegalArgumentException(
                        "Could not determine db schema for " + javaType + "." );
            }
            
            final TableName tableNameAnnotation = javaType.getAnnotation( TableName.class );
            final String tableName = ( null == tableNameAnnotation ) ?
                    toDatabaseColumnName( javaType.getSimpleName() )
                    : tableNameAnnotation.value();
            return schemaName + "." + tableName;
        }
    } // end inner class def
    
    
    private final static class DatabaseEnumNameCacheResultProvider 
        implements CacheResultProvider< Class< ? >, String >
    {
        public String generateCacheResultFor( final Class< ? > javaType )
        {
            final String schemaName = getSchemaName( javaType );
            if ( null == schemaName )
            {
                throw new IllegalArgumentException(
                        "Could not determine db schema for " + javaType + "." );
            }
            
            return schemaName 
                   + "." 
                   + toDatabaseColumnName( javaType.getSimpleName() );
        }
    } // end inner class def
    
    
    private final static class JavaTypeNameCacheResultProvider
        implements CacheResultProvider< String, String >
    {
        public String generateCacheResultFor( String databaseTypeName )
        {
            if ( databaseTypeName.contains( "." ) )
            {
                // strip off the schema name in front
                databaseTypeName = databaseTypeName.split( "\\." )[ 1 ];
            }
            final String retval = toBeanPropertyName( databaseTypeName );
            return retval.substring( 0, 1 ).toUpperCase() + retval.substring( 1 );
        }
    } // end inner class def
    
    
    private final static class JavaTypeSchemaNameCacheResultProvider
        implements CacheResultProvider< Class< ? >, String >
    {
        public String generateCacheResultFor( final Class< ? > javaType )
        {
            if ( null == javaType )
            {
                return null;
            }
            
            final Schema schema = javaType.getAnnotation( Schema.class );
            if ( null != schema )
            {
                return schema.value();
            }
            
            final String [] packageName = javaType.getPackage().getName().split( "\\." );
            return packageName[ packageName.length - 1 ];
        }
    } // end inner class def
    

    private final static StaticCache< Class< ? >, String > DATABASE_TABLE_NAME_CACHE =
            new StaticCache<>( new DatabaseTableNameCacheResultProvider() );
    private final static StaticCache< Class< ? >, String > DATABASE_ENUM_NAME_CACHE =
            new StaticCache<>( new DatabaseEnumNameCacheResultProvider() );
    private final static StaticCache< Class< ? >, String > JAVA_TYPE_SCHEMA_NAME_CACHE =
            new StaticCache<>( new JavaTypeSchemaNameCacheResultProvider() );
    private final static StaticCache< String, String > JAVA_TYPE_NAME_CACHE =
            new StaticCache<>( new JavaTypeNameCacheResultProvider() );
}
