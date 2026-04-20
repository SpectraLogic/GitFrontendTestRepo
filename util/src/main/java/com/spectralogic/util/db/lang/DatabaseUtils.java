/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.security.ChecksumGenerator;

public final class DatabaseUtils
{
    private DatabaseUtils()
    {
        // singleton
    }
    
    
    public static String getChecksum( final String sql )
    {
        final StringBuilder checksumString = new StringBuilder();
        for ( final String line : sql.split( Platform.NEWLINE ) )
        {
            if ( line.startsWith( "--" ) || line.trim().isEmpty() )
            {
                continue;
            }
            checksumString.append( line );
        }
        return ChecksumGenerator.generateMd5( checksumString.toString() );
    }
    
    
    public static long getNextTransactionNumber()
    {
        return TRANSACTION_NUMBER.incrementAndGet();
    }
    
    
    public static String getTransactionDescription( final Long transactionNumber )
    {
        if ( null == transactionNumber )
        {
            return "";
        }
        return "SQLTrans-" + transactionNumber;
    }
    
    
    public static String getPrimaryKeyPropertyName( final Class< ? > type )
    {
        if ( DatabasePersistable.class.isAssignableFrom( type ) )
        {
            return Identifiable.ID;
        }
        throw new UnsupportedOperationException( "No code written to handle " + type );
    }
    
    
    public static Set< String > getPersistablePropertyNames( final Class< ? > type )
    {
        return PERSISTABLE_PROPERTY_NAMES_CACHE.get( type );
    }
    
    
    public static boolean doesPropertyHaveUniqueConstraint(
            final Class< ? > type,
            final String propertyName )
    {
        final UniqueIndexes uniqueIndexesAnnotation = type.getAnnotation( UniqueIndexes.class );
        if ( uniqueIndexesAnnotation != null )
        {
            for ( final Unique uniqueIndex : uniqueIndexesAnnotation.value() )
            {
                final String[] columns = uniqueIndex.value();
                if ( columns.length == 1 && columns[0].equals( propertyName ) )
                {
                    return true;
                }
            }
        }
        return false;
    }
    
    
    private final static class PersistablePropertyNamesCacheResultProvider 
        implements CacheResultProvider< Class< ? >, Set< String > >
    {
        public Set< String > generateCacheResultFor( final Class< ? > type )
        {
            final Set< String > retval = BeanUtils.getPropertyNames( type );
            for ( final String prop : new HashSet<>( retval ) )
            {
                if ( !isPersistableToDatabase( type, prop ) )
                {
                    retval.remove( prop );
                }
            }
            return retval;
        }
    } // end inner class def
    
    
    public static boolean isPersistableToDatabase(
            Class< ? > type,
            final String propertyName )
    {
        type = BeanFactory.getType( type );
        if ( !DatabasePersistable.class.isAssignableFrom( type ) )
        {
            return false;
        }
        
        final Method reader = BeanUtils.getReader( type, propertyName );
        final Method writer = BeanUtils.getWriter( type, propertyName );
        if ( null != reader && null != reader.getAnnotation( ExcludeFromDatabasePersistence.class ) )
        {
            return false;
        }
        
        if ( null != writer && null != writer.getAnnotation( ExcludeFromDatabasePersistence.class ) )
        {
            return false;
        }
        
        if ( null == reader )
        {
            throw new IllegalArgumentException( "Reader missing on " + type + "." + propertyName );
        }
        if ( null == writer )
        {
            throw new IllegalArgumentException( "Writer missing on " + type + "." + propertyName );
        }
        
        return true;
    }
    
    
    public static String toDatabaseType( Class<?> javaType, boolean autoIncrementing )
    {
        javaType = ReflectUtil.toNonPrimitiveType( javaType );
        
        if ( String.class == javaType )
        {
            return "varchar";
        }
        if ( Integer.class == javaType )
        {
            if (autoIncrementing) return "serial";
            return "integer";
        }
        if ( Long.class == javaType )
        {
            if (autoIncrementing) return "bigserial";
            return "bigint";
        }
        if ( Double.class == javaType )
        {
            return "double precision";
        }
        if ( UUID.class == javaType )
        {
            return "uuid";
        }
        if ( Date.class == javaType )
        {
            return "timestamp without time zone";
        }
        if ( Boolean.class == javaType )
        {
            return "boolean";
        }
        if ( javaType.isEnum() )
        {
            return DatabaseNamingConvention.toDatabaseEnumName( javaType );
        }
        
        throw new UnsupportedOperationException( "No code to support " + javaType );
    }
    
    
    public static int getLogLevel( 
            Class< ? > type, 
            final SqlOperation operation, 
            final TransactionLogLevel transactionLogLevel )
    {
        type = BeanFactory.getType( type );
        final ConfigureSqlLogLevels customLogLevels = type.getAnnotation( ConfigureSqlLogLevels.class );
        if ( null == customLogLevels )
        {
            transactionLogLevel.add( operation.getDefaultLogLevel() );
            return operation.getDefaultLogLevel();
        }
        
        transactionLogLevel.add( customLogLevels.value().getLogLevel( operation ) );
        return customLogLevels.value().getLogLevel( operation );
    }
    
    
    public final static String NOTIFICATION_MESSAGE_SEPARATOR = "!";
    
    private final static StaticCache< Class< ? >, Set< String > > PERSISTABLE_PROPERTY_NAMES_CACHE =
            new StaticCache<>( new PersistablePropertyNamesCacheResultProvider() );
    private final static AtomicLong TRANSACTION_NUMBER = new AtomicLong( 0 );
}
