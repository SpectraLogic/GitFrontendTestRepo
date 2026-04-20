/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.validate;

import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.codegen.SqlCodeGenerator;
import com.spectralogic.util.db.domain.*;
import com.spectralogic.util.db.lang.*;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DataSourceValidator implements Runnable
{
    public DataSourceValidator(
            final DataManager dataManager, 
            final Connection connection,
            final Set< Class< ? extends DatabasePersistable > > dataTypes )
    {
        m_dataManager = dataManager;
        m_connection = connection;
        m_dataTypes = dataTypes;
    }
    
    
    public void run()
    {
        try
        {
            runInternal();
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    /**
     * If you DROP or ALTER a DB table during a test (unit, integration, etc.),
     * then just before needing to interact with it again (for example, through
     * any method of DatabaseSupport or DataManager), clear this class's test
     * support performance cache of already validated backing DB tables. Do this
     * because at least one table in the cache (the one you just messed with) is
     * no longer valid.
     */
    public static void clearAlreadyValidatedTablesCache()
    {
        synchronized( ALREADY_VALIDATED_TABLES )
        {
            ALREADY_VALIDATED_TABLES.clear();
        }
    }
    
    
    private void runInternal() throws SQLException
    {
        if( ALREADLY_VALIDATED_CACHE_ENABLED.get() )
        {
            synchronized ( ALREADY_VALIDATED_TABLES )
            {
                if ( ! ALREADY_VALIDATED_TABLES.containsAll( m_dataTypes ) )
                {
                    runValidations();
                    ALREADY_VALIDATED_TABLES.addAll( m_dataTypes );
                }
            }
        }
        else
        {
            runValidations();
        }
    }
    
    
    private void runValidations() throws SQLException
    {
        LOG.info( "Validating data source..." );
        verifyCanGenerateDatabaseSchema();
        verifyCanTalk();
        verifyDatabaseEncoding();
        verifyDatabaseDefinedMatchesJavaDefinitions();
        verifyCorrectOrderByBehavior();
        ensureAllEnumConstantsDefined();
        LOG.info( "Data source is valid." );
    }
    
    
    private void verifyCanTalk() throws SQLException
    {
        final ResultSet rs = m_connection.prepareStatement( "SELECT version() AS version;" ).executeQuery();
        if ( !rs.next() )
        {
            throw new RuntimeException( "Failed to retrieve version." );
        }
        final String version = rs.getString( "version" );
        LOG.info( "Data source communication established.  Talking to server version '" + version + "'." );
        
        if ( !version.contains( REQUIRED_POSTGRES_SERVER_VERSION ) )
        {
            throw new RuntimeException(
                    "The required version of Postgres is " + REQUIRED_POSTGRES_SERVER_VERSION 
                    + "*, but the version running is " + version + "." );
        }
    }
    
    
    private void verifyDatabaseEncoding() throws SQLException
    {
        final ResultSet rs = m_connection.prepareStatement( "show server_encoding;" ).executeQuery();
        if ( !rs.next() )
        {
            throw new RuntimeException( "Failed to retrieve version." );
        }
        final String encoding = rs.getString( "server_encoding" );
        if ( !"UTF8".equals( encoding ) )
        {
            throw new RuntimeException( "Server encoding is " + encoding + "." );
        }
        rs.close();
    }
    
    
    private void ensureAllEnumConstantsDefined() throws SQLException
    {
        for ( final Class< ? > type : m_dataTypes )
        {
            ensureAllEnumConstantsDefinedInternal( type );
        }
    }
    
    
    private void ensureAllEnumConstantsDefinedInternal( final Class< ? > type ) throws SQLException
    {
        if ( SimpleBeanSafeToProxy.class.isAssignableFrom( type ) )
        {
            for ( final String prop : BeanUtils.getPropertyNames( type ) )
            {
                final Method reader = BeanUtils.getReader( type, prop );
                if ( null == reader )
                {
                    continue;
                }
                ensureAllEnumConstantsDefinedInternal( reader.getReturnType() );
            }
            return;
        }
        if ( !type.isEnum() )
        {
            return;
        }

        final String dbTypeName = DatabaseNamingConvention.toDatabaseTableName( type );
        for ( final Object constant : type.getEnumConstants() )
        {
            m_connection.prepareStatement(
                    "ALTER TYPE " + dbTypeName + " ADD VALUE IF NOT EXISTS '" + constant + "'" )
                    .execute();
        }
    }
    
    
    private void verifyCorrectOrderByBehavior()
    {
        final List< TestRecord > keyValues = new ArrayList<>();
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "a" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "B" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "c" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_a" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_B" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_c" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "a1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "B1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "c1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "11" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_a1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_B1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_c1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_11" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "az1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "Bz1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "cz1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "1z1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_az1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_Bz1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_cz1" ) );
        keyValues.add( BeanFactory.newBean( TestRecord.class ).setKey( getClass().getName() + "_1z1" ) );

        Collections.sort( keyValues, new BeanComparator<>( TestRecord.class, TestRecord.KEY ) );
        
        final List< String > javaNames = new ArrayList<>();
        for ( final TestRecord kv : keyValues )
        {
            javaNames.add( kv.getKey() );
        }
        
        m_dataManager.deleteBeans( 
                TestRecord.class,
                Require.beanPropertyEqualsOneOf( 
                        TestRecord.KEY,
                        javaNames ) );
        for ( final TestRecord kv : new HashSet<>( keyValues ) )
        {
            m_dataManager.createBean( kv );
        }
    
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( TestRecord.KEY, SortBy.Direction.ASCENDING );
        final List< TestRecord > dbKeyValues = m_dataManager.getBeans(
                TestRecord.class, Query.where( Require.beanPropertyEqualsOneOf( 
                        TestRecord.KEY,
                        javaNames ) ).orderBy( ordering ) ).toList();
        final List< String > dbNames = new ArrayList<>();
        for ( final TestRecord kv : dbKeyValues )
        {
            dbNames.add( kv.getKey() );
        }
        m_dataManager.deleteBeans( 
                TestRecord.class,
                Require.beanPropertyEqualsOneOf( 
                        TestRecord.KEY,
                        javaNames ) );
        if ( !javaNames.equals( dbNames ) )
        {
            throw new RuntimeException( 
                    "The database is sorting results differently from Java application code: " 
                    + javaNames + " vs " + dbNames );
        }
    }
    
    
    private void verifyCanGenerateDatabaseSchema()
    {
        new SqlCodeGenerator( new HashSet<>( m_dataTypes ), "doesntmatter" );
        LOG.info( m_dataManager.getClass().getSimpleName() + " has a valid and complete data type set." );
    }
    
    
    private void verifyDatabaseDefinedMatchesJavaDefinitions()
    {
        for ( final Class< ? extends DatabasePersistable > dataType : m_dataTypes )
        {
            try
            {
                if (!DatabaseView.class.isAssignableFrom(dataType)) {
                    verifyDatabaseDefinedMatches(dataType);
                } else {
                    LOG.warn("Will not verify table view " + dataType.getSimpleName() + " because it is a view, not a table.");
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to validate " + dataType, ex );
            }
        }
        LOG.info( "Data source table definitions are compatible with loaded Java definitions." );
    }
    
    
    private void verifyDatabaseDefinedMatches( final Class< ? > type )
    {
        //get all fkey constraints for this table
        final Set<TableConstraints> constraints = m_dataManager.getBeans(
                TableConstraints.class,
                Query.where( Require.all(
                        Require.beanPropertyEquals(
                                TableConstraints.TABLE_SCHEMA,
                                DatabaseNamingConvention.getSchemaName( type ) ),
                        Require.beanPropertyEquals(
                                TableConstraints.CONSTRAINT_TYPE,
                                "FOREIGN KEY"),
                        Require.beanPropertyEquals(
                                TableConstraints.TABLE_NAME,
                                DatabaseNamingConvention.toDatabaseTableName( type ).split( "\\." )[1] ) ) ) )
                                            .toSet();
        //Build a map of associations between column name and referenced type for these fkeys.
        final Map<String, String> columnToReferencedType= new HashMap<>();
        for ( final TableConstraints fkeyConstraint : constraints) {
            final List<KeyColumnUsage> keyColumnUsages = m_dataManager.getBeans(
                    KeyColumnUsage.class,
                            Query.where( Require.beanPropertyEquals(
                                    KeyColumnUsage.CONSTRAINT_NAME,
                                    fkeyConstraint.getConstraintName() ) ) ).toList();
            final List<ConstraintTableUsage> constraintTableUsages = m_dataManager.getBeans(
                    ConstraintTableUsage.class,
                    Query.where( Require.beanPropertyEquals(
                            ConstraintTableUsage.CONSTRAINT_NAME,
                            fkeyConstraint.getConstraintName() ) ) ).toList();

            if (keyColumnUsages.size() != 1) {
                throw new IllegalStateException("Expected to find 1 column usage for constraint " + fkeyConstraint.getConstraintName() + " but found " + keyColumnUsages.size());
            }
            if (constraintTableUsages.size() != 1) {
                throw new IllegalStateException("Expected to find 1 table usage for constraint " + fkeyConstraint.getConstraintName() + " but found " + constraintTableUsages.size());
            }

            final String columnName = keyColumnUsages.get(0).getColumnName();
            final String referencedTable = constraintTableUsages.get(0).getTableName();

            columnToReferencedType.put(columnName, referencedTable);
        }
        final Set< Column > columns = m_dataManager.getBeans(
                Column.class,
                Query.where( Require.all( 
                        Require.beanPropertyEquals(
                                Column.TABLE_SCHEMA,
                                DatabaseNamingConvention.getSchemaName( type ) ),
                        Require.beanPropertyEquals( 
                                Column.TABLE_NAME, 
                                DatabaseNamingConvention.toDatabaseTableName( type ).split( "\\." )[1] ) ) ) )
                                            .toSet();

        final boolean readonly = ReadOnly.class.isAssignableFrom( type );
        final Set< String > javaProps = DatabaseUtils.getPersistablePropertyNames( type );
        for ( final Column c : columns )
        {
            final String prop = DatabaseNamingConvention.toBeanPropertyName( c.getColumnName() );
            final String description = c.getTableSchema() + "." + c.getTableName() + "." + c.getColumnName();
            if ( !javaProps.remove( prop ) )
            {
                if ( readonly )
                {
                    continue;
                }
                throw new IllegalStateException( "Unexpected column in database: " + description );
            }
            
            try
            {
                verifyColumnDefinedMatches( readonly, type, c, prop, columnToReferencedType.get(c.getColumnName() ) );
            }
            catch ( final Exception ex )
            {
                throw new IllegalStateException( "Validation failed for " + description + ".", ex );
            }
        }
        
        if ( !javaProps.isEmpty() )
        {
            throw new IllegalStateException(
                    "The following columns were missing (but expected): " + javaProps );
        }
    }
    
    
    private void verifyColumnDefinedMatches( 
            final boolean readonly,
            final Class< ? > type, 
            final Column c, 
            final String prop,
            final String dbReferencedType )
    {

        verifyColumnDataType( c.getDataType(), type, prop );
        
        if ( readonly )
        {
            return;
        }

        verifyColumnNullability( c.getIsNullable(), type, prop );
        verifyColumnForeignKey( dbReferencedType, type, prop );
    }
    
    
    private void verifyColumnDataType( final String dbDataType, final Class< ? > type, final String prop )
    {
        final Method reader = BeanUtils.getReader( type, prop );
        if ( "uuid".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, UUID.class );
        }
        else if ( "integer".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, Integer.class );
        }
        else if ( "bigint".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, Long.class );
        }
        else if ( "double precision".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, Double.class );
        }
        else if ( "boolean".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, Boolean.class );
        }
        else if ( "character varying".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, String.class );
        }
        else if ( "timestamp without time zone".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, Date.class );
        }
        else if ( "name".equals( dbDataType ) )
        {
            BeanUtils.verifyReaderReturnType( type, prop, String.class );
        }
        else if ( "USER-DEFINED".equals( dbDataType ) )
        {
            if ( !reader.getReturnType().isEnum() )
            {
                throw new IllegalArgumentException( "Expected reader return type to be an enum." );
            }
        }
        else
        {
            throw new UnsupportedOperationException( "No code written to support " + dbDataType + "." );
        }
    }
    
    
    private void verifyColumnNullability( 
            final String dbNullability, final Class< ? > type, final String prop )
    {
        final Method reader = BeanUtils.getReader( type, prop );
        if ( "YES".equals( dbNullability ) )
        {
            if ( null == reader.getAnnotation( Optional.class ) )
            {
                throw new IllegalStateException( "Column should not have allowed nulls." );
            }
        }
        else if ( "NO".equals( dbNullability ) )
        {
            if ( null != reader.getAnnotation( Optional.class ) )
            {
                throw new IllegalStateException( "Column should have allowed nulls." );
            }
        }
        else
        {
            throw new UnsupportedOperationException( "No code written to handle: " + dbNullability );
        }
    }


    private void verifyColumnForeignKey(final String dbReferencedType, final Class< ? > type, final String prop )
    {
        final Method reader = BeanUtils.getReader( type, prop );
        final References annotation = reader.getAnnotation( References.class );
        final String referencedType = annotation == null ? null : DatabaseNamingConvention.toDatabaseTableName( annotation.value() ).split( "\\." )[1];
        if (!Objects.equals(referencedType, dbReferencedType)) {
            throw new IllegalStateException("Column should have referenced type " + referencedType + ", but database fkey is " + dbReferencedType);
        }
    }
    
    
    private final DataManager m_dataManager;
    private final Connection m_connection;
    private final Set< Class< ? extends DatabasePersistable > > m_dataTypes;
    
    private final static Logger LOG = Logger.getLogger( DataSourceValidator.class );
    private final static String REQUIRED_POSTGRES_SERVER_VERSION = "18";
    
    /**
     * Update this constant ONLY during test runs. NEVER access nor change
     * the value of this constant in any non-test run context.
     */
    private static final AtomicBoolean ALREADLY_VALIDATED_CACHE_ENABLED =
                                                     new AtomicBoolean( false );
    public  static final String  ALRREADY_VALIDATED_CACHE_ENABLED_FIELD =
                                              "ALREADLY_VALIDATED_CACHE_ENABLED";
    
    private final static Set< Class< ? extends DatabasePersistable > >  
              ALREADY_VALIDATED_TABLES = Collections.synchronizedSet(
                      new HashSet< Class< ? extends DatabasePersistable > >() );
}
