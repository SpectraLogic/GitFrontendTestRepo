/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.lang.SecretSqlParameter;
import com.spectralogic.util.db.lang.SqlOperation;
import com.spectralogic.util.db.lang.TransactionLogLevel;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.lang.Validations;

/**
 * Marshals Java beans to the database
 */
public final class DatabaseMarshaler
{
    public DatabaseMarshaler( final DatabasePersistable bean, final TransactionLogLevel transactionLogLevel )
    {
        m_bean = bean;
        m_transactionLogLevel = transactionLogLevel;
        Validations.verifyNotNull( "Bean", bean );
        Validations.verifyNotNull( "Transaction log level", transactionLogLevel );
    }
    
    
    public void create( final Long transactionNumber, final ConnectionPool connectionPool )
    {
        CreateUpdateBeanValidator.validate( null, m_bean );
        final List< Object > sqlParams = new ArrayList<>();
        final String sql = "INSERT INTO " + DatabaseNamingConvention.toDatabaseTableName( m_bean.getClass() )
                + generateInsertColumnsAndValues( sqlParams );

        SqlStatementExecutor.executeCommit( 
                transactionNumber,
                connectionPool, 
                sql, 
                sqlParams,
                DatabaseUtils.getLogLevel( m_bean.getClass(), SqlOperation.INSERT, m_transactionLogLevel ) );
    }
    
    
    private String generateInsertColumnsAndValues( final List< Object > sqlParams )
    {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        
        boolean isFirst = true;
        for ( final String prop : DatabaseUtils.getPersistablePropertyNames( m_bean.getClass() ) )
        {
            final Method reader = BeanUtils.getReader( m_bean.getClass(), prop );
            Object value;
            try
            {
                value = reader.invoke( m_bean );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            if ( null == value )
            {
                continue;
            }
            
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                columns.append( ", " );
                values.append( ", " );
            }
            
            columns.append( DatabaseNamingConvention.toDatabaseColumnName( prop ) );
            if ( reader.getReturnType().isEnum() )
            {
                values.append( 
                 "CAST(? AS " + DatabaseNamingConvention.toDatabaseEnumName( reader.getReturnType() ) + ")" );
            }
            else
            {
                values.append( "?" );
            }

            if ( reader.getReturnType().isEnum() )
            {
                value = value.toString();
            }
            if ( null != reader.getAnnotation( Secret.class ) )
            {
                value = new SecretSqlParameter( value );
            }
            
            sqlParams.add( value );
        }
        
        return "(" + columns + ") VALUES (" + values + ")";
    }
    
    
    public String getCopyBasedCreateEntry()
    {
        CreateUpdateBeanValidator.validate( null, m_bean );
        final StringBuilder retval = new StringBuilder();
        final List< String > propNames = 
                new ArrayList<>( DatabaseUtils.getPersistablePropertyNames( m_bean.getClass() ) );
        Collections.sort( propNames );
        boolean isFirstProp = true;
        for ( final String propName : propNames )
        {
            final Method reader = BeanUtils.getReader( m_bean.getClass(), propName );
            Object value;
            try
            {
                value = reader.invoke( m_bean );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            
            if ( isFirstProp )
            {
                isFirstProp = false;
            }
            else
            {
                retval.append( "|" );
            }
            if ( null == value )
            {
                retval.append( "\\N" );
            }
            else
            {
                if ( reader.getReturnType().isEnum() )
                {
                    value = value.toString();
                }
                appendEscapedString( retval, value.toString() );
            }
        }
        
        return retval.toString();
    }
    
    
    private static StringBuilder appendEscapedString(
            final StringBuilder stringBuilder,
            final String data )
    {
        final int length = data.length();
        for ( int i = 0; i < length; ++i )
        {
            final char c = data.charAt( i );

            switch ( c )
            {
                case '\\':
                case '|':
                    stringBuilder.append( '\\' ).append( c );
                    break;

                case '\b':
                    stringBuilder.append( "\\b" );
                    break;
                case '\f':
                    stringBuilder.append( "\\f" );
                    break;
                case '\n':
                    stringBuilder.append( "\\n" );
                    break;
                case '\r':
                    stringBuilder.append( "\\r" );
                    break;
                case '\t':
                    stringBuilder.append( "\\t" );
                    break;
                case '\u000b':
                    stringBuilder.append( "\\v" );
                    break;
                    
                default:
                    stringBuilder.append( c );
                    break;
            }
        }
        return stringBuilder;
    }


    public void update(
            final Long transactionNumber,
            final ConnectionPool connectionPool,
            final Set< String > propertiesToUpdate, 
            final WhereClause whereClause )
    {
        CreateUpdateBeanValidator.validate( propertiesToUpdate, m_bean );
        final List< Object > sqlParams = new ArrayList<>();
        final String sql = "UPDATE " + DatabaseNamingConvention.toDatabaseTableName( m_bean.getClass() )
                + " SET " + generateUpdateSql( propertiesToUpdate, sqlParams )
                + " WHERE " + whereClause.toSql(
                        BeanFactory.getType( m_bean.getClass() ),
                        sqlParams );

         SqlStatementExecutor.executeCommit(
                 transactionNumber,
                 connectionPool,
                 sql,
                 sqlParams,
                 DatabaseUtils.getLogLevel( m_bean.getClass(), SqlOperation.UPDATE, m_transactionLogLevel ) );
    }
    
    
    private String generateUpdateSql(
            final Set< String > propertiesToUpdate, 
            final List< Object > sqlParams )
    {
        final StringBuilder retval = new StringBuilder();
        
        boolean isFirst = true;
        for ( final String prop : propertiesToUpdate )
        {
            final Method reader = BeanUtils.getReader( m_bean.getClass(), prop );
            Object value;
            try
            {
                value = reader.invoke( m_bean );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                retval.append( ", " );
            }
            
            retval.append( DatabaseNamingConvention.toDatabaseColumnName( prop ) + " = " );
            if ( reader.getReturnType().isEnum() )
            {
                retval.append( 
                 "CAST(? AS " + DatabaseNamingConvention.toDatabaseEnumName( reader.getReturnType() ) + ")" );
            }
            else
            {
                retval.append( "?" );
            }
            
            if ( reader.getReturnType().isEnum() && null != value )
            {
                value = value.toString();
            }
            if ( null != reader.getAnnotation( Secret.class ) )
            {
                value = new SecretSqlParameter( value );
            }
            
            sqlParams.add( value );
        }
        
        return retval.toString();
    }
    
    
    private final DatabasePersistable m_bean;
    private final TransactionLogLevel m_transactionLogLevel;
}
