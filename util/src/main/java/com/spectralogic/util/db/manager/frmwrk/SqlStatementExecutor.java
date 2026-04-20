/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.lang.SecretSqlParameter;
import com.spectralogic.util.db.manager.DatabaseErrorCodes;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class SqlStatementExecutor
{
    private SqlStatementExecutor()
    {
        // singleton
    }
    
    
    public static Executor executeQuery( 
            final Long transactionNumber,
            final ConnectionPool connectionPool, 
            final String sql, 
            final List< Object > parameters,
            final int logLevel )
    {
        Validations.verifyNotNull( "Connection pool", connectionPool );
        final Executor executor = new Executor(
                transactionNumber,
                connectionPool, 
                sql,
                parameters,
                logLevel );
        try
        {
            return executor.executeQuery();
        }
        catch ( final RuntimeException ex )
        {
            executor.close();
            throw ex;
        }
    }
    
    
    public static void executeCommit( 
            final Long transactionNumber,
            final ConnectionPool connectionPool, 
            final String sql, 
            final List< Object > parameters,
            final int logLevel )
    {
        Validations.verifyNotNull( "Connection pool", connectionPool );
        final Executor executor = new Executor(
                transactionNumber,
                connectionPool,
                sql,
                parameters,
                logLevel );
        try
        {
            executor.executeUpdate();
        }
        catch ( final RuntimeException ex )
        {
            DatabaseErrorCodes.verifyConstraintViolation( ex.getMessage(), ex );
            throw ex;
        }
        finally
        {
            executor.close();
        }
    }
    
    
    public static void executeCopy(
            final long transactionNumber,
            final Class< ? extends DatabasePersistable > type,
            final ConnectionPool connectionPool,
            final List< String > copyCommandContents,
            final int logLevel )
    {
        Validations.verifyNotNull( "Connection pool", connectionPool );
        final List< String > propNames = 
                new ArrayList<>( DatabaseUtils.getPersistablePropertyNames( type ) );
        Collections.sort( propNames );
        
        String columns = " (";
        for ( int i = 0; i < propNames.size(); i++ )
        {
            columns = columns + 
                    DatabaseNamingConvention.toDatabaseColumnName( propNames.get( i ) ) + ", ";
        }
        columns = columns.substring( 0, columns.length() - 2 ) + ")";
        
        final String sql = "COPY " + DatabaseNamingConvention.toDatabaseTableName( type ) 
              + columns + " FROM STDIN WITH DELIMITER AS '|'";
        final Connection connection = connectionPool.takeConnection();
        /*
         * Postgres 13.3 had a problem when, most obviously, a bucket is deleted and then 250k objects are inserted
         * into a new bucket. The query plan during the COPY is producing a sequence scan of tables rather than
         * an index scan.  This forces a re-plan during COPY operations.
         */
        try
        {
            connection.createStatement()
                      .execute( "DISCARD PLANS" );
        }
        catch ( SQLException e )
        {
            LOG.error( "Failed to set discard plans, continuing: ", e );
        }
        try
        {
            final PipedInputStream is = new PipedInputStream( 1024 * 1024 * 4 );
            try ( final PipedOutputStream os = new PipedOutputStream( is ) )
            {
                final PrintWriter pw = new PrintWriter( os );
                final CopyManager copyManager = new CopyManager( (BaseConnection)connection );
                final Future< ? > copySqlProducerFuture = SystemWorkPool.getInstance().submit( new Runnable()
                {
                    public void run()
                    {
                        for ( final String s : copyCommandContents )
                        {
                            pw.write( s + Platform.NEWLINE );
                        }
                        pw.close();
                    }
                } );
            
                final ReadableSql readableSql = new ReadableSql( Long.valueOf( transactionNumber ), logLevel );
                readableSql.append( sql, false );
                final MonitoredWork monitoredWork = new MonitoredWork( 
                        StackTraceLogging.NONE, readableSql.getLogMessagePreExecution() );
                try
                {
                    final Duration duration = new Duration();
                    final long numberOfRows = copyManager.copyIn( sql, is );
                    readableSql.log( duration, Integer.valueOf( (int)numberOfRows ) );
                    copySqlProducerFuture.get();
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( "Failed to execute copy.", ex );
                }
                finally
                {
                    monitoredWork.completed();
                }
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to execute copy: " + sql, ex );
        }
        finally
        {
            connectionPool.returnConnection( connection );
        }
    }
    
    
    public static String getReadableSql(
            final Class< ? extends DatabasePersistable > clazz, 
            final WhereClause whereClause )
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = whereClause.toSql( clazz, sqlParameters );
        
        return new Executor( 
                null,
                null,
                sql, 
                sqlParameters, 
                Priority.INFO_INT ).m_readableSql.getLogMessagePreExecution();
    }
    
    
    public final static class Executor
    {
        private Executor( 
                final Long transactionNumber,
                final ConnectionPool connectionPool, 
                final String sql, 
                final List< Object > parameters,
                final int logLevel )
        {
            m_readableSql = new ReadableSql( transactionNumber, logLevel );
            m_connectionPool = connectionPool;
            m_connection = ( null == m_connectionPool ) ? null : m_connectionPool.takeConnection();
            m_transactionNumber = transactionNumber;
            
            final Set< Integer > secretParameterIndexes = new HashSet<>();
            try
            {
                final PreparedStatement retval = 
                        ( null == m_connection ) ? null : m_connection.prepareStatement( sql );
                for ( int i = 0; i < parameters.size(); ++i )
                {
                    Object param = parameters.get( i );
                    if ( null != param && SecretSqlParameter.class == param.getClass() )
                    {
                        param = ( (SecretSqlParameter)param ).getParameter();
                        secretParameterIndexes.add( Integer.valueOf( i ) );
                    }
                    
                    if ( null != retval )
                    {
                        if ( null != param && Date.class.isAssignableFrom( param.getClass() ) )
                        {
                            retval.setTimestamp( i + 1, new java.sql.Timestamp( ((Date)param ).getTime() ) );
                        }
                        else if ( null != param && param.getClass().isEnum() )
                        {
                            retval.setString( i + 1, param.toString() );
                        }
                        else if ( null != param && param.getClass().isArray() )
                        {
                            retval.setArray( i + 1, m_connection.createArrayOf(
                                    DatabaseUtils.toDatabaseType( param.getClass().getComponentType(), false ),
                                    (Object[])param ) );
                        }
                        else
                        {
                            retval.setObject( i + 1, param );
                        }
                    }
                }
                
                int paramNumber = -1;
                for ( int i = 0; i < sql.length(); ++i )
                {
                    if ( '?' == sql.charAt( i ) )
                    {
                        if ( secretParameterIndexes.contains( Integer.valueOf( ++paramNumber ) ) )
                        {
                            m_readableSql.append( "{CONCEALED}" );
                        }
                        else
                        { 
                            final Class< ? > clazz = ( null == parameters.get( paramNumber ) ) ? 
                                    null : parameters.get( paramNumber ).getClass();
                            final String quote = ( null == clazz ) ? "" :
                                    ( Number.class.isAssignableFrom( clazz ) 
                                      || Boolean.class.isAssignableFrom( clazz ) ) ? "" : "'";
                            
                            String readableSqlParameter = ( null == clazz ) ? 
                                    "" 
                                    : ( clazz.isArray() && !clazz.getComponentType().isPrimitive() ) ? 
                                            Arrays.toString( (Object[])parameters.get( paramNumber ) )
                                            : parameters.get( paramNumber ).toString();
                            m_readableSql.append( quote + readableSqlParameter + quote );
                        }
                    }
                    else
                    {
                        m_readableSql.append( String.valueOf( sql.charAt( i ) ) );
                    }
                }
                
                m_statement = retval;
            }
            catch ( final SQLException ex )
            {
                throw new RuntimeException( "Failed to generate sql statement.", ex );
            }
        }
        
        
        private Executor executeQuery()
        {
            final MonitoredWork monitoredWork = new MonitoredWork( 
                    StackTraceLogging.NONE, m_readableSql.getLogMessagePreExecution() );
            try
            {
                m_closeTransactionUponClose = ( null == m_transactionNumber );
                m_statement.setFetchSize( 10000 );
                final Duration duration = new Duration();
                m_resultSet = m_statement.executeQuery();
                m_readableSql.log( duration, null );
                return this;
            }
            catch ( final SQLException ex )
            {
                throw new DaoException( 
                        "Failed to execute: " + m_readableSql.getLogMessagePreExecution(),
                        ex );
            }
            finally
            {
                monitoredWork.completed();
            }
        }
        
        
        private Executor executeUpdate()
        {
            final MonitoredWork monitoredWork = new MonitoredWork(
                    StackTraceLogging.NONE, m_readableSql.getLogMessagePreExecution() );
            try
            {
                final Duration duration = new Duration();
                final int retval = m_statement.executeUpdate();
                m_readableSql.log( duration, Integer.valueOf( retval ) );
                return this;
            }
            catch ( final SQLException ex )
            {
                throw new DaoException( 
                        "Failed to execute: " + m_readableSql,
                        ex );
            }
            finally
            {
                monitoredWork.completed();
            }
        }
        
        
        public ResultSet getResultSet()
        {
            if ( null == m_resultSet )
            {
                throw new IllegalStateException( "Result set not set." );
            }
            
            return m_resultSet;
        }
        
        
        public void close()
        {
            try
            {
                if ( null != m_resultSet )
                {
                    m_resultSet.close();
                }
                m_statement.close();
                if ( m_closeTransactionUponClose )
                {
                    m_connection.commit();
                }
                m_connectionPool.returnConnection( m_connection );
            }
            catch ( final SQLException ex )
            {
                LOG.warn( "Failed to cleanup on close.", ex );
            }
        }
        
        
        private volatile boolean m_closeTransactionUponClose;
        private volatile ResultSet m_resultSet;
        
        private final ReadableSql m_readableSql;
        private final PreparedStatement m_statement;
        private final Connection m_connection;
        private final ConnectionPool m_connectionPool;
        private final Long m_transactionNumber;
    } // end inner class def
    
    
    private final static class ReadableSql
    {
        private ReadableSql( final Long transactionNumber, final int logLevel )
        {
            final String prefix;
            if ( null == transactionNumber )
            {
                prefix = "SQL: ";
            }
            else
            {
                prefix = DatabaseUtils.getTransactionDescription( transactionNumber ) + ": ";
            }
            
            m_logLevelInt = logLevel;
            m_logLevel = Level.toLevel( m_logLevelInt );
            
            m_shortMessage.append( prefix );
            if ( DbLogger.DB_SQL_LOG.isDebugEnabled() )
            {
                m_fullMessage = new StringBuilder( prefix );
            }
            else
            {
                m_fullMessage = null;
            }
        }
        
        
        private void append( String value )
        {
            append( value, true );
        }
        
        
        private void append( String value, final boolean truncateForShortMessage )
        {
            if ( null != m_fullMessage )
            {
                m_fullMessage.append( value );
            }
            
            if ( truncateForShortMessage && 50 < value.length() )
            {
                value =
                      value.substring( 0, 20 ) 
                      + "...{" + ( value.length() - 30 ) + " chars" + "}..." 
                      + value.substring( value.length() - 10 );
            }
            m_shortMessage.append( value );
        }
        
        
        private void log( final Duration duration, final Integer numberOfRowsAffected )
        {
            if ( LOG.isEnabledFor( m_logLevel ) && ( LOG_IF_AFFECTS_ZERO_ROWS || numberOfRowsAffected > 0) )
            {
                LOG.log( m_logLevel, getLogMessage( m_shortMessage, duration, numberOfRowsAffected, 500 ) );
            }
            
            if ( null != m_fullMessage )
            {
                DbLogger.DB_SQL_LOG.debug( 
                        getLogMessage( m_fullMessage, duration, numberOfRowsAffected, Integer.MAX_VALUE ) );
            }
        }
        
        
        private String getLogMessage(
                final StringBuilder message,
                final Duration duration, 
                final Integer numberOfRowsAffected,
                final int maxLength )
        {
            final String suffix;
            if ( null == numberOfRowsAffected )
            {
                suffix = " [" + duration + "]";
            }
            else
            {
                suffix = " [" + duration + ", " + numberOfRowsAffected + " rows affected]";
            }
            
            final String retval = message.toString() + suffix;
            if ( retval.length() > maxLength )
            {
                return retval.substring( 0, maxLength - 10 ) + "...";
            }
            return LogUtil.getShortVersion( retval );
        }
        
        
        private String getLogMessagePreExecution()
        {
            final String retval = m_shortMessage.toString();
            if ( retval.length() > 400 )
            {
                return retval.substring( 0, 390 ) + "...";
            }
            return retval;
        }
        
        
        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + getLogMessagePreExecution() + "]";
        }


        private final Level m_logLevel;
        private final int m_logLevelInt;
        private final StringBuilder m_shortMessage = new StringBuilder();
        private final StringBuilder m_fullMessage;
    } // end inner class def
    
    
    private final static Logger LOG = Logger.getLogger( SqlStatementExecutor.class );
    private final static boolean LOG_IF_AFFECTS_ZERO_ROWS = false;
}
