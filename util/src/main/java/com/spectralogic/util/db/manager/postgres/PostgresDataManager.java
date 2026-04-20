/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.db.manager.postgres;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.domain.Column;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.lang.ReadOnly;
import com.spectralogic.util.db.lang.SqlOperation;
import com.spectralogic.util.db.lang.TransactionLogLevel;
import com.spectralogic.util.db.lang.TransactionLogLevelImpl;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.DatabaseErrorCodes;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.db.manager.frmwrk.ConnectionPool;
import com.spectralogic.util.db.manager.frmwrk.ConnectionPoolFactory;
import com.spectralogic.util.db.manager.frmwrk.DatabaseMarshaler;
import com.spectralogic.util.db.manager.frmwrk.DatabaseUnmarshaler;
import com.spectralogic.util.db.manager.frmwrk.DbLogger;
import com.spectralogic.util.db.manager.frmwrk.SingleConnectionConnectionPool;
import com.spectralogic.util.db.manager.frmwrk.SqlStatementExecutor;
import com.spectralogic.util.db.manager.frmwrk.SqlStatementExecutor.Executor;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Query.Retrievable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.validate.DataSourceNuker;
import com.spectralogic.util.db.validate.DataSourceValidator;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.find.FlagDetector;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.lang.iterate.StreamedResultsProvider;
import com.spectralogic.util.lang.iterate.StreamingIterable;
import com.spectralogic.util.lang.iterate.StreamingIterator;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class PostgresDataManager extends BaseShutdownable implements DataManager
{
    /**
     * @param connectionPoolSize - Size of the pool
     * @param seedClassesInPackagesToSearchForDataTypes - Provide at least one class in each package you want
     * searched recursively for the data types to be supported by this data manager.  The set of all 
     * {@link com.spectralogic.util.db.lang.DatabasePersistable} Java Beans in the searched packages and
     * subpackages will be the supported data model.
     */
    public PostgresDataManager(
            final int connectionPoolSize,
            final Set< Class< ? > > seedClassesInPackagesToSearchForDataTypes )
    {
        Validations.verifyInRange( "Connection pool size", 3, Integer.MAX_VALUE, connectionPoolSize );
        Validations.verifyNotNull( "Packages to search", seedClassesInPackagesToSearchForDataTypes );
        
        final Set< Class< ? extends DatabasePersistable > > types = new HashSet<>();
        for ( final Class< ? > clazz : seedClassesInPackagesToSearchForDataTypes )
        {
            types.addAll( DATABASE_PERSISTABLES_CACHE.get( clazz ) );
        }
        
        LOG.info( 
                types.size() 
                + " business domain data types found that will be supported by this data manager." );
        types.addAll( DATABASE_PERSISTABLES_CACHE.get( Column.class ) );
        LOG.info( 
                types.size() 
                + " total data types found that will be supported by this data manager." );
        
        m_connectionPoolSize = connectionPoolSize;
        m_dataTypes = types;
    }
    
    
    private PostgresDataManager( final PostgresDataManager source )
    {
        m_connectionPoolSize = source.m_connectionPoolSize;
        m_dataTypes = new HashSet<>( source.m_dataTypes );
    }
    
    

    public Set< Class< ? extends DatabasePersistable > > getSupportedTypes()
    {
        return new HashSet<>( m_dataTypes );
    }
    
    
    public int getCount( final Class< ? extends DatabasePersistable > type, final Retrievable retrievable )
    {
        Validations.verifyNotNull( "Type", type );
        Validations.verifyNotNull( "Retrievable", retrievable );
        verifyCanServiceDataType( type, false );
        
        try
        {
            return ( int ) getAggregateInternal( "COUNT(*)", type, retrievable );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( ex );
        }
    }
    
    
    public int getCount( final Class< ? extends DatabasePersistable > type, final WhereClause whereClause )
    {
        Validations.verifyNotNull( "Type", type );
        Validations.verifyNotNull( "Filter", whereClause );
        verifyCanServiceDataType( type, false );
        try
        {
            return (int)getAggregateInternal( "COUNT(*)", type, whereClause );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( ex );
        }
    }
    
    
    public long getMax( 
            final Class< ? extends DatabasePersistable > type,
            final String maxPropertyName,
            final WhereClause whereClause )
    {
        return getAggregateValue( "MAX", type, maxPropertyName, whereClause );
    }
    
    
    public long getMin( 
            final Class< ? extends DatabasePersistable > type,
            final String maxPropertyName,
            final WhereClause whereClause )
    {
        return getAggregateValue( "MIN", type, maxPropertyName, whereClause );
    }
    
    
    public long getSum( 
            final Class< ? extends DatabasePersistable > type,
            final String maxPropertyName,
            final WhereClause whereClause )
    {
        return getAggregateValue( "SUM", type, maxPropertyName, whereClause );
    }
    
    
    private long getAggregateValue( 
            final String aggregateFunctionName,
            final Class< ? extends DatabasePersistable > type,
            final String aggregatePropertyName,
            final WhereClause whereClause )
    {
        Validations.verifyNotNull( "Aggregate function name", aggregateFunctionName );
        Validations.verifyNotNull( "Type", type );
        Validations.verifyNotNull( "Property name", aggregatePropertyName );
        Validations.verifyNotNull( "Filter", whereClause );
        verifyCanServiceDataType( type, false );
        try
        {
            return getAggregateInternal(
                    aggregateFunctionName 
                        + "(" + DatabaseNamingConvention.toDatabaseColumnName( aggregatePropertyName ) + ")",
                    type,
                    whereClause );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( ex );
        }
    }
    
    
    private long getAggregateInternalResults( final String aggregateSelectText, final Executor executor )
    throws SQLException
    {
        try
        {
            final ResultSet resultSet = executor.getResultSet();
            if ( !resultSet.next() )
            {
                throw new RuntimeException( "Failed to retrieve " + aggregateSelectText + "." );
            }
            return resultSet.getLong( 1 );
        }
        finally
        {
            executor.close();
        }
    }
    
    
    private long getAggregateInternal( final String aggregateSelectText,
            final Class< ? extends DatabasePersistable > type, final Retrievable retrievable ) throws SQLException
    {
        final List< Object > params = new ArrayList<>();
        final String sql =
                "SELECT " + aggregateSelectText + " FROM (" + retrievable.toSql( type, params ) + ") AS " + "agg";
    
        final Executor executor =
                SqlStatementExecutor.executeQuery( m_transactionNumber, m_explicitCommitConnectionPool, sql,
                params,
                DatabaseUtils.getLogLevel( type, SqlOperation.SELECT, m_transactionLogLevel ) );
        return getAggregateInternalResults( aggregateSelectText, executor );
    }
    
    
    private long getAggregateInternal( final String aggregateSelectText,
            final Class< ? extends DatabasePersistable > type, final WhereClause whereClause ) throws SQLException
    {
        final List< Object > params = new ArrayList<>();
        final String sql =
                "SELECT " + aggregateSelectText + " FROM " + DatabaseNamingConvention.toDatabaseTableName( type ) +
                        " WHERE " + whereClause.toSql( type, params );
        
        final Executor executor =
                SqlStatementExecutor.executeQuery( m_transactionNumber, m_explicitCommitConnectionPool, sql, params,
                        DatabaseUtils.getLogLevel( type, SqlOperation.SELECT, m_transactionLogLevel ) );
        return getAggregateInternalResults( aggregateSelectText, executor );
    }

    
    public < T extends DatabasePersistable > EnhancedIterable< T > getBeans(
            final Class< T > type,
            final Retrievable retrievable )
    {
        Validations.verifyNotNull( "Type", type );
        Validations.verifyNotNull( "Retrievable", retrievable );
        verifyCanServiceDataType( type, false );
        
        try
        {
            return getBeansInternal( type, retrievable );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( ex );
        }
    }

    
    private < T extends DatabasePersistable > EnhancedIterable< T > getBeansInternal(
            final Class< T > type,
            final Retrievable retrievable )
    {
        final List< Object > params = new ArrayList<>();
        final String sql = retrievable.toSql( type, params );
        final Executor executor = SqlStatementExecutor.executeQuery( 
                m_transactionNumber,
                m_explicitCommitConnectionPool,
                sql, 
                params, 
                DatabaseUtils.getLogLevel( type, SqlOperation.SELECT, m_transactionLogLevel ) );
        
        final StreamingIterator< T > retval = new StreamingIterator<>(
                new GetBeansStreamedResultsProvider<>( type, executor, new DatabaseUnmarshaler<>( type ) ) );
        return new StreamingIterable<>( retval );
    }
    
    
    private final static class GetBeansStreamedResultsProvider< T extends DatabasePersistable > 
        implements StreamedResultsProvider< T >
    {
        private GetBeansStreamedResultsProvider( 
                final Class< T > type,
                final Executor executor,
                final DatabaseUnmarshaler< T > unmarshaler )
        {
            m_executor = executor;
            m_unmarshaler = unmarshaler;
            
            m_work = new MonitoredWork( 
                    StackTraceLogging.NONE, 
                    "Get " + type.getSimpleName() + " beans: " 
                    + ExceptionUtil.getLimitedStackTrace( Thread.currentThread().getStackTrace(), 16 ) )
            .withCustomLogger( DbLogger.DB_HOG_LOG );
        }
        
        public T getNextResult()
        {
            if ( m_closed )
            {
                throw new IllegalStateException( "Cannot get next result once done." );
            }
            
            try
            {
                if ( !m_executor.getResultSet().next() )
                {
                    close();
                    return null;
                }
                return m_unmarshaler.unmarshal( m_executor.getResultSet() );
            }
            catch ( final Exception ex )
            {
                close();
                throw new RuntimeException( ex );
            }
        }
        
        public void close()
        {
            if ( m_closed )
            {
                return;
            }
            
            m_closed = true;
            m_work.completed();
            m_executor.close();
        }
        
        private final MonitoredWork m_work;
        private volatile boolean m_closed;
        private final Executor m_executor;
        private final DatabaseUnmarshaler< T > m_unmarshaler;
    } // end inner class def
    

    public < T extends DatabasePersistable > T discover( 
            final Class< T > type,
            final Object someBeanPropertyValue )
    {
        Validations.verifyNotNull( "Type", type );
        Validations.verifyNotNull( "Bean property value", someBeanPropertyValue );
        verifyCanServiceDataType( type, false );
        
        final Set< UUID > retvalIds = new HashSet<>();
        final List< T > retval = new ArrayList<>();
        for ( int i = 0; i < 3; ++i )
        {
            final Set< WhereClause > filters = getFindBeanFiltersForDiscovery( 
                    type, 
                    someBeanPropertyValue,
                    ( 0 == i ), 
                    ( 1 == i ),
                    ( 2 == i ) );
            
            for ( final WhereClause filter : filters )
            {
                try
                {
                    final Set< T > beans = getBeans( type, Query.where( filter ) ).toSet();
                    if ( !beans.isEmpty() )
                    {
                        LOG.info( "Discovered " + beans.size() + " " + type.getSimpleName() 
                                   + " where " + filter + "." );
                    }
                    for ( final T bean : beans )
                    {
                        if ( retvalIds.add( bean.getId() ) )
                        {
                            retval.add( bean );
                        }
                    }
                }
                catch ( final RuntimeException ex )
                {
                    Validations.verifyNotNull( "Shut up codepro", ex );
                    LOG.debug( "It is invalid to attempt to discover a " + type.getSimpleName()
                               + " where " + filter + ".  Will attempt to discover via other properties." );
                }
            }
            
            if ( 1 < retval.size() )
            {
                throw new DaoException( 
                        GenericFailure.MULTIPLE_RESULTS_FOUND,
                        retval.size() + " results found when searching for a " + type 
                        + " with identifier or bean property value " + someBeanPropertyValue + "." );
            }
            if ( 1 == retval.size() )
            {
                return retval.get( 0 );
            }
        }
        throw new DaoException(
                GenericFailure.NOT_FOUND, 
                type + " not found via identifier / bean property value '" + someBeanPropertyValue + "'." );
    }
    
    
    private Set< WhereClause > getFindBeanFiltersForDiscovery( 
            final Class< ? extends DatabasePersistable > clazz,
            final Object value,
            final boolean includePrimaryKeys,
            final boolean includeUniqueKeys,
            final boolean includeOtherKeys )
    {
        final Set< WhereClause > filters = new HashSet<>();
        for ( final String prop : DatabaseUtils.getPersistablePropertyNames( clazz ) )
        {
            final Method reader = BeanUtils.getReader( clazz, prop );
            final boolean isPrimaryKey = ( prop.equals( DatabaseUtils.getPrimaryKeyPropertyName( clazz ) ) );
            final boolean isUniqueKey = DatabaseUtils.doesPropertyHaveUniqueConstraint( clazz, prop );
            final boolean isOtherKey = !( isPrimaryKey || isUniqueKey );
            
            if ( ( includePrimaryKeys && isPrimaryKey )
                 || ( includeUniqueKeys && isUniqueKey )
                 || ( includeOtherKeys && isOtherKey ) )
            {
                if ( null != reader.getAnnotation( Secret.class ) )
                {
                    continue;
                }
                filters.add( Require.beanPropertyEquals( prop, value ) );
            }
        }
        
        return filters;
    }


    public < T extends DatabasePersistable > void createBean( final T bean )
    {
        Validations.verifyNotNull( "Bean", bean );
        verifyCanServiceDataType( bean.getClass(), true );
    
        checkForDisabledWrites();
    
        try
        {
            final DatabaseMarshaler marshaler = new DatabaseMarshaler( bean, m_transactionLogLevel );
            populatePrimaryKey( bean );
            marshaler.create( m_transactionNumber, m_autoCommitConnectionPool );
        }
        catch ( final Exception ex )
        {
            final String failureMessage = "Failed to create " + bean + ".";
            DatabaseErrorCodes.verifyConstraintViolation( failureMessage, ex );
            throw new DaoException( failureMessage, ex );
        }
    }
    
    
    private void checkForDisabledWrites()
    {
        if ( disableWrites.get() )
        {
            throw new DaoException( GenericFailure.INTERNAL_ERROR,
                    "Database writes are disabled due to lack of disk space. Please contact Spectra Support." );
        }
    }
    
    
    private void populatePrimaryKey( final DatabasePersistable bean )
    {
        final Class< ? extends DatabasePersistable > type = bean.getClass();
        try
        {
            final String primaryKeyPropName = DatabaseUtils.getPrimaryKeyPropertyName( type );
            final Method reader = BeanUtils.getReader( type, primaryKeyPropName );
            
            final Object value = reader.invoke( bean );
            if ( null != value )
            {
                return;
            }
            if ( UUID.class != reader.getReturnType() )
            {
                throw new UnsupportedOperationException( 
                        "Cannot auto-generate primary key " + primaryKeyPropName + "." );
            }
        
            BeanUtils.getWriter( bean.getClass(), primaryKeyPropName ).invoke( 
                    bean, 
                    UUID.randomUUID() );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( "Failed to populate primary key for: " + bean, ex );
        }
    }
    
    
    public < T extends DatabasePersistable > void createBeans( final Set< T > beans )
    {
        Validations.verifyNotNull( "Beans", beans );
        if ( null == m_transactionNumber )
        {
            throw new IllegalStateException(
                    "Bulk creation must be done within the context of a transaction.  This ensures that " 
                    + "either all the beans were created or none of the beans were created." );
        }
    
        checkForDisabledWrites();
    
        if ( MAX_BEANS_PER_CREATE_BEANS_COMMAND >= beans.size() )
        {
            createBeansInternal( beans );
        }
        else
        {
            final Set< T > chunk = new HashSet<>();
            for ( final T bean : beans )
            {
                chunk.add( bean );
                if ( MAX_BEANS_PER_CREATE_BEANS_COMMAND == chunk.size() )
                {
                    createBeansInternal( chunk );
                    chunk.clear();
                }
            }
            createBeansInternal( chunk );
        }
    }
    
    
    private < T extends DatabasePersistable > void createBeansInternal( final Set< T > beans )
    {
        if ( beans.isEmpty() )
        {
            return;
        }
    
        checkForDisabledWrites();
    
        final Class< ? extends DatabasePersistable > type = beans.iterator().next().getClass();
        verifyCanServiceDataType( type, true );

        try
        {
            final List< String > copyText = new ArrayList<>();
            for ( final T bean : beans )
            {
                final DatabaseMarshaler marshaler = new DatabaseMarshaler( bean, m_transactionLogLevel );
                populatePrimaryKey( bean );
                copyText.add( marshaler.getCopyBasedCreateEntry() );
            }
    
            SqlStatementExecutor.executeCopy( m_transactionNumber,
                    type, 
                    m_autoCommitConnectionPool, 
                    copyText,
                    DatabaseUtils.getLogLevel( type, SqlOperation.INSERT, m_transactionLogLevel ) );
        }
        catch ( final Exception ex )
        {
            final String failureMessage = "Failed to create: " + LogUtil.getShortVersion( beans.toString() );
            DatabaseErrorCodes.verifyConstraintViolation( failureMessage, ex );
            throw new DaoException( failureMessage, ex );
        }
    }


    public < T extends DatabasePersistable > void updateBean( 
            final Set< String > propertiesToUpdate, 
            final T bean )
    {
        Validations.verifyNotNull( "Bean", bean );
    
        checkForDisabledWrites();
    
        updateBeans( 
                ( null == propertiesToUpdate ) ? 
                        DatabaseUtils.getPersistablePropertyNames( bean.getClass() ) 
                        : propertiesToUpdate,
                bean, 
                getSingleBeanResultFilter( bean ) );
    }
    

    public < T extends DatabasePersistable > void updateBeans( 
            final Set< String > propertiesToUpdate, 
            final T bean,
            final WhereClause whereClause )
    {
        Validations.verifyNotNull( "Properties to update", propertiesToUpdate );
        Validations.verifyNotNull( "Filter", whereClause );
        Validations.verifyNotNull( "Bean with modifications to make", bean );
        verifyCanServiceDataType( bean.getClass(), true );
    
        checkForDisabledWrites();
    
        new DatabaseMarshaler( bean, m_transactionLogLevel ).update( 
                m_transactionNumber,
                m_autoCommitConnectionPool,
                propertiesToUpdate,
                whereClause );
    }
    

    public < T extends DatabasePersistable > void deleteBean( final Class< T > type, final UUID id )
    {
        deleteBeans( 
                type,
                Require.beanPropertyEquals( DatabaseUtils.getPrimaryKeyPropertyName( type ), id ) );
    }
    
    
    public < T extends DatabasePersistable > void deleteBeans( 
            final Class< T > type,
            final WhereClause whereClause )
    {
        Validations.verifyNotNull( "Type", type );
        Validations.verifyNotNull( "Filter", whereClause );
        verifyCanServiceDataType( type, true );
        String sql = null;
        try
        {
            final List< Object > sqlParameters = new ArrayList<>();
            sql = "DELETE FROM " + DatabaseNamingConvention.toDatabaseTableName( type )
                    + " WHERE " + whereClause.toSql( type, sqlParameters );
            SqlStatementExecutor.executeCommit(
                    m_transactionNumber,
                    m_autoCommitConnectionPool, 
                    sql, 
                    sqlParameters,
                    DatabaseUtils.getLogLevel( type, SqlOperation.DELETE, m_transactionLogLevel ) );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( 
                    GenericFailure.CONFLICT,
                    "Delete failed (does something depend on what we tried to delete?): " + sql, ex );
        }
    }
    
    
    public < T extends DatabasePersistable > void truncate( final Class< T > type )
    {
        Validations.verifyNotNull( "Type", type );
        verifyCanServiceDataType( type, true );
        String sql = null;
        try
        {
            final List< Object > sqlParameters = new ArrayList<>();
            sql = "TRUNCATE " + DatabaseNamingConvention.toDatabaseTableName( type ) + " CASCADE";
            SqlStatementExecutor.executeCommit(
                    m_transactionNumber,
                    m_autoCommitConnectionPool, 
                    sql, 
                    sqlParameters,
                    DatabaseUtils.getLogLevel( type, SqlOperation.DELETE, m_transactionLogLevel ) );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( "Delete failed: " + sql, ex );
        }
    }
    
    
    private WhereClause getSingleBeanResultFilter( final DatabasePersistable bean )
    {
        Validations.verifyNotNull( "Bean", bean );
        final List< WhereClause > filters = new ArrayList<>();

        try
        {
            final String primaryPropertyName = DatabaseUtils.getPrimaryKeyPropertyName( bean.getClass() );
            final Object value = BeanUtils.getReader( bean.getClass(), primaryPropertyName ).invoke( bean );
            if ( null == value )
            {
                throw new DaoException( 
                        GenericFailure.INTERNAL_ERROR,
                        bean.getClass() + "." + primaryPropertyName + " must be specified." );
            }
            filters.add( Require.beanPropertyEquals(
                    primaryPropertyName,
                    value ) );
        }
        catch ( final Exception ex )
        {
            throw new DaoException( ex );
        }
        
        return Require.all( filters );
    }
    
    
    public File getDataDirectory()
    {
        if ( null == m_dataDirectory )
        {
            throw new IllegalStateException( "Data directory not calculated yet." );
        }
        return m_dataDirectory;
    }
    
    
    public DatabasePhysicalSpaceState getFreeToTotalDiskSpaceRatioState()
    {
        return DatabasePhysicalSpaceState.getFreeToTotalDiskSpaceRatioState(
                getDataDirectory().getFreeSpace() * 1.0f / getDataDirectory().getTotalSpace() );
    }
    
    
    public DatabasePhysicalSpaceState getMaxTableToFreeRatioState()
    {
        final long tableBytes = getMaxTableBytes();
        final long diskFreeSpace = getDataDirectory().getFreeSpace();
        return DatabasePhysicalSpaceState.getFreeToTotalDiskSpaceRatioState(
                ( diskFreeSpace - tableBytes ) * 1.0f / diskFreeSpace );
    }
    
    
    @Override public DatabasePhysicalSpaceState getDatabaseSpaceState()
    {
        final DatabasePhysicalSpaceState freeSpaceState = getFreeToTotalDiskSpaceRatioState();
        final DatabasePhysicalSpaceState tableSpaceState = getMaxTableToFreeRatioState();
        
        if ( freeSpaceState.getFreeSpaceRatioToReachThreshold() < tableSpaceState.getFreeSpaceRatioToReachThreshold() )
        {
            return freeSpaceState;
        }
        else
        {
            return tableSpaceState;
        }
    }
    
    
    private long getMaxTableBytes()
    {
        long tableBytes = 0;
        final Connection tableSizeConnection = m_autoCommitConnectionPool.takeConnection();
        try
        {
            final ResultSet rs = tableSizeConnection.prepareStatement(
                    "SELECT table_schema, table_name, row_estimate, total_bytes, index_bytes, total_bytes - " +
                            "index_bytes - COALESCE(toast_bytes, 0) AS table_bytes FROM (SELECT c.oid, nspname AS " +
                            "table_schema, relname AS table_name, c.reltuples AS row_estimate, pg_total_relation_size" +
                            "(c.oid) AS total_bytes, pg_indexes_size(c.oid) AS index_bytes, pg_total_relation_size" +
                            "(reltoastrelid) AS toast_bytes FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c" +
                            ".relnamespace WHERE relkind = 'r') a ORDER BY table_bytes DESC LIMIT 1" )
                                                    .executeQuery();
            if ( !rs.next() )
            {
                throw new RuntimeException( "Failed to retrieve table sizes." );
            }
            tableBytes = rs.getLong( "table_bytes" );
            final String tableSchema = rs.getString( "table_schema" );
            final String tableName = rs.getString( "table_name" );
            final long totalBytes = rs.getLong( "total_bytes" );
            final long indexBytes = rs.getLong( "index_bytes" );
            final long rowEstimate = rs.getLong( "row_estimate" );
            
            LOG.info( "Largest database table information: " + tableSchema + "." + tableName + " row estimate = " +
                    rowEstimate + ", data size = " + new BytesRenderer().render( tableBytes ) + ", index size = " +
                    new BytesRenderer().render( indexBytes ) + ", total size = " +
                    new BytesRenderer().render( totalBytes ) );
        }
        catch ( final SQLException e )
        {
            LOG.error( "Failed to execute query to get table sizes: " + e.getMessage() );
        }
        finally
        {
            m_autoCommitConnectionPool.returnConnection( tableSizeConnection );
        }
        return tableBytes;
    }
    
    
    public void setDataSource( final DataSource dataSource )
    {
        setDataSourceInternal( dataSource, true );

        final Executor executor = SqlStatementExecutor.executeQuery(
                null,
                m_explicitCommitConnectionPool, 
                "SHOW data_directory",
                new ArrayList<>(), 
                Priority.DEBUG_INT );
        try
        {
            executor.getResultSet().next();
            m_dataDirectory = new File( executor.getResultSet().getString( 1 ) );
            if ( !m_dataDirectory.exists() )
            {
                m_dataDirectory = null;
            }
        }
        catch ( final Exception ex )
        {
            throw new DaoException( "Failed to determine data directory.", ex );
        }
        finally
        {
            executor.close();
        }
        
        final long totalSpace = getDataDirectory().getTotalSpace();
        final BytesRenderer renderer = new BytesRenderer();
        LOG.info( 
                "Database resides at " + getDataDirectory().getAbsolutePath() + ":" 
                + Platform.NEWLINE + "       Total Space: "
                + renderer.render( totalSpace )
                + Platform.NEWLINE + "        Free Space: " 
                + renderer.render( getDataDirectory().getFreeSpace() ) 
                + " (" + getFreeToTotalDiskSpaceRatioState() + ")"
                + Platform.NEWLINE + "Near Low Threshold: "
                + renderer.render( getPhysicalSpaceThreshold( 
                        totalSpace, DatabasePhysicalSpaceState.NEAR_LOW ) )
                + Platform.NEWLINE + "     Low Threshold: "
                + renderer.render( getPhysicalSpaceThreshold( 
                        totalSpace, DatabasePhysicalSpaceState.LOW ) )
                + Platform.NEWLINE + "Critical Threshold: " 
                + renderer.render( getPhysicalSpaceThreshold( 
                        totalSpace, DatabasePhysicalSpaceState.CRITICAL ) ) );
        
        m_healthChecker.run();
        m_healthCheckerExecutor.start();
    }
    
    
    private long getPhysicalSpaceThreshold( final long totalSpace, final DatabasePhysicalSpaceState state )
    {
        return (long)( totalSpace * state.getFreeSpaceRatioToReachThreshold() );
    }
    
    
    synchronized private void setDataSourceInternal( final DataSource dataSource, final boolean validate )
    {
        if ( null != m_autoCommitConnectionPool )
        {
            throw new IllegalStateException( "Data source already set." );
        }
        
        try
        {
            m_autoCommitConnectionPool =
                   ConnectionPoolFactory.getInstance().getPool(
                                    dataSource,
                                    true, 
                                    Connection.TRANSACTION_READ_UNCOMMITTED, 
                                    m_connectionPoolSize, 
                                    15 * 60 * 1000 );
            m_explicitCommitConnectionPool =
                   ConnectionPoolFactory.getInstance().getPool(
                                    dataSource, 
                                    false, 
                                    Connection.TRANSACTION_READ_UNCOMMITTED,
                                    m_connectionPoolSize, 
                                    15 * 60 * 1000 );
                   
            addShutdownListener( m_autoCommitConnectionPool );
            addShutdownListener( m_explicitCommitConnectionPool );
            addShutdownListener( m_healthCheckerExecutor );
            
            final Connection validatorConnection = m_autoCommitConnectionPool.takeConnection();
            if ( validate )
            {
                try
                {
                    new DataSourceValidator( this, validatorConnection, m_dataTypes ).run();
                }
                catch ( final RuntimeException ex )
                {
                    m_incompatibleDataSource = dataSource;
                    if ( FlagDetector.isFlagSet( DataSourceNuker.ENABLE_NUKER_FLAG ) )
                    {
                        new DataSourceNuker( this, validatorConnection, m_dataTypes ).run();
                        new DataSourceValidator( this, validatorConnection, m_dataTypes ).run();
                    }
                    else
                    {
                        shutdown();
                        throw ex;
                    }
                }
            }
            m_autoCommitConnectionPool.returnConnection( validatorConnection );
        }
        catch ( final Exception ex )
        {
            LOG.error( 
                    "Could not use data source.  Shutting down to prevent damage to the data source.", ex );
            shutdown();
            throw new IllegalArgumentException( 
                    "Could not use data source.  Shut down to prevent damage to the data source.", ex );
        }
    }
    
    
    synchronized private void setDataSourceForTransaction( final ConnectionPool connectionPool )
    {
        if ( null != m_autoCommitConnectionPool )
        {
            throw new IllegalStateException( "Data source already set." );
        }
        
        m_autoCommitConnectionPool = connectionPool;
        m_explicitCommitConnectionPool = connectionPool;
        addShutdownListener( m_autoCommitConnectionPool );
        doNotLogWhenShutdown();
    }
    
    
    public DataManager toUnsafeModeForIncompatibleDataSource()
    {
        if ( null == m_incompatibleDataSource )
        {
            throw new IllegalStateException( 
                    "This method can only be called after an incompatible data source has been set." );
        }
        
        final PostgresDataManager retval = new PostgresDataManager( this );
        retval.setDataSourceInternal( m_incompatibleDataSource, false );
        return retval;
    }
    
    
    private void verifyCanServiceDataType( Class< ? > type, final boolean requiresWriteAccess )
    {
        type = BeanFactory.getType( type );
        if ( !m_dataTypes.contains( type ) )
        {
            throw new IllegalStateException( type + " is not loaded as a supported data type." );
        }
        if ( requiresWriteAccess && ReadOnly.class.isAssignableFrom( type ) )
        {
            throw new DaoException( GenericFailure.FORBIDDEN, type + " cannot be modified" );
        }
        if ( null == m_autoCommitConnectionPool )
        {
            throw new IllegalStateException( "No data source has been set yet." );
        }
        verifyNotShutdown();
    }
    
    
    public DataManager startTransaction()
    {
        verifyNotShutdown();
        final PostgresDataManager retval = new PostgresDataManager( this );
        final SingleConnectionConnectionPool transactionConnectionPool =
                new SingleConnectionConnectionPool( m_explicitCommitConnectionPool );
        retval.setDataSourceForTransaction( transactionConnectionPool );
        retval.m_transactionNumber = DatabaseUtils.getNextTransactionNumber();
        return retval;
    }
    
    
    public void commitTransaction()
    {
        verifyNotShutdown();
        if ( null == m_transactionNumber )
        {
            throw new IllegalStateException( "This is not a transaction." );
        }

        final Duration duration = new Duration();
        final Connection connection = m_explicitCommitConnectionPool.takeConnection();
        try
        {
            connection.commit();
            LOG.log( m_transactionLogLevel.getLevel(),
                     DatabaseUtils.getTransactionDescription( m_transactionNumber ) 
                     + ": Commit successful [" + duration + "]" );
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( 
                    DatabaseUtils.getTransactionDescription( m_transactionNumber ) + ": Commit FAILED", ex );
        }
        finally
        {
            m_explicitCommitConnectionPool.returnConnection( connection );
            shutdown();
        }
    }
    
    
    public void closeTransaction()
    {
        if ( null == m_transactionNumber )
        {
            throw new IllegalStateException( "This is not a transaction." );
        }
        if ( isShutdown() )
        {
            return;
        }

        final Duration duration = new Duration();
        final Connection connection = m_explicitCommitConnectionPool.takeConnection();
        try
        {
            connection.rollback();
            LOG.log( m_transactionLogLevel.getLevel(),
                     DatabaseUtils.getTransactionDescription( m_transactionNumber ) 
                     + ": Rollback successful [" + duration + "]" );
        }
        catch ( final SQLException ex )
        {
            LOG.warn( 
                    DatabaseUtils.getTransactionDescription( m_transactionNumber ) + ": Rollback FAILED", 
                    ex );
        }
        finally
        {
            shutdown();
        }
    }
    
    
    private final class PhysicalDatabaseLocationHealthChecker implements Runnable
    {
        synchronized public void run()
        {
            if ( null == m_physicalLocationOfDatabase )
            {
                m_physicalLocationOfDatabase = getDataDirectory();
            }

            switch ( getFreeToTotalDiskSpaceRatioState() )
            {
                case CRITICAL:
                    LOG.error( getMessage( "critical" ) );
                    disableWrites.set( true );
                    break;
                case LOW:
                    LOG.warn( getMessage( "running low" ) );
                    disableWrites.set( false );
                    break;
                case NEAR_LOW:
                    LOG.warn( getMessage( "near running low" ) );
                    disableWrites.set( false );
                    break;
                case NORMAL:
                    disableWrites.set( false );
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "No code to handle: " + getFreeToTotalDiskSpaceRatioState() );
            }
    
            switch ( getMaxTableToFreeRatioState() )
            {
                case CRITICAL:
                    LOG.error( getTableMessage( "critical" ) );
                    disableWrites.set( true );
                    break;
                case LOW:
                    LOG.warn( getTableMessage( "running low" ) );
                    disableWrites.set( false );
                    break;
                case NEAR_LOW:
                    LOG.warn( getTableMessage( "near running low" ) );
                    disableWrites.set( false );
                    break;
                case NORMAL:
                    disableWrites.set( false );
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "No code to handle: " + getFreeToTotalDiskSpaceRatioState() );
            }
        }
    
    
        private String getMessage( final String status )
        {
            return "Free to total disk space ratio is " + status +
                    ", thus the database physical space can be affected (free space remaining is "
                   + new BytesRenderer().render( getDataDirectory().getFreeSpace() ) + " ).";
        }
    
    
        private String getTableMessage( final String status )
        {
            return "Maximum database table size (" + new BytesRenderer().render( getMaxTableBytes() ) +
                    ") to total disk space (" + new BytesRenderer().render( getDataDirectory().getFreeSpace() ) +
                    ") ratio is " + status + ", thus database operations can be affected.";
        }
        
        private File m_physicalLocationOfDatabase;
    } // end inner class def
    
    
    public void reserveConnections(
            final int numberOfAutoCommitConnections,
            final int numberOfExplicitCommitConnections )
    {
        if ( null != m_transactionNumber )
        {
            throw new IllegalStateException( "Connections cannot be reserved on a connection." );
        }
        m_autoCommitConnectionPool.reserveConnections( numberOfAutoCommitConnections );
        m_explicitCommitConnectionPool.reserveConnections( numberOfExplicitCommitConnections );
    }
    
    
    public void releaseReservedConnections()
    {
        m_autoCommitConnectionPool.releaseReservedConnections();
        m_explicitCommitConnectionPool.releaseReservedConnections();
    }
    
    
    private static final
        StaticCache< Class< ? >, Set< Class< ? extends DatabasePersistable > > >
            DATABASE_PERSISTABLES_CACHE = new StaticCache<>(
                new CacheResultProvider<
                       Class< ? >, Set< Class< ? extends DatabasePersistable > > >()
    {
        @SuppressWarnings( "unchecked" )
        public Set< Class< ? extends DatabasePersistable >>
                                     generateCacheResultFor( Class< ? > seed )
        {
            final String pkg = seed.getPackage().getName();
            LOG.info( new StringBuilder( 150 )
                             .append( "Finding " )
                             .append( DatabasePersistable.class.getSimpleName() )
                             .append( " types in " ).append(  pkg ) );
            
            final PackageContentFinder finder =
                                      new PackageContentFinder( pkg, seed, null );
    
            final Set< ? > classes = finder.getClasses(
                    element -> DatabasePersistable.class.isAssignableFrom( element ) && element.isInterface() );
            
            if ( classes.isEmpty() )
            {
                throw new IllegalArgumentException( "Failed to find any types." );
            }
            
            return (Set< Class< ? extends DatabasePersistable > >)classes;
        }
    } );


    private volatile DataSource m_incompatibleDataSource;
    private volatile ConnectionPool m_autoCommitConnectionPool;
    private volatile ConnectionPool m_explicitCommitConnectionPool;
    private volatile Long m_transactionNumber;
    private volatile File m_dataDirectory;
    
    private final Set< Class< ? extends DatabasePersistable > > m_dataTypes;
    private final int m_connectionPoolSize;
    private final TransactionLogLevel m_transactionLogLevel = new TransactionLogLevelImpl();
    private final PhysicalDatabaseLocationHealthChecker m_healthChecker =
            new PhysicalDatabaseLocationHealthChecker();
    private final RecurringRunnableExecutor m_healthCheckerExecutor = 
            new RecurringRunnableExecutor( m_healthChecker, 60000 );
    
    private final static int MAX_BEANS_PER_CREATE_BEANS_COMMAND = 10000;
    private final static Logger LOG = Logger.getLogger( PostgresDataManager.class );
    private final AtomicBoolean disableWrites = new AtomicBoolean( false );
} 
