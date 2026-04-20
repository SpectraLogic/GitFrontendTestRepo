/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.validate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.db.codegen.SqlCodeGenerator;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.ReadOnly;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class DataSourceNuker implements Runnable
{
    public DataSourceNuker(
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
            new DataSourceValidator( m_dataManager, m_connection, m_dataTypes ).run();
            LOG.info( "Database is compatible.  No need to nuke it." );
        }
        catch ( final Exception ex )
        {
            LOG.warn( LogUtil.getLogMessageCriticalBlock(
                    "Database is incompatible.  Will proceed anyway by nuking it.", 5 ), ex );
            m_dataManager = m_dataManager.toUnsafeModeForIncompatibleDataSource();
            nukeDatabase();
            createDatabase();
        }
    }
    
    
    private void nukeDatabase()
    {
        LOG.info( "Nuking database..." );
        
        for ( final Class< ? extends DatabasePersistable > type : m_dataManager.getSupportedTypes() )
        {
            if ( ReadOnly.class.isAssignableFrom( type ) )
            {
                continue;
            }
            
            try
            {
                m_oldDatabase.put(
                        type,
                        m_dataManager.getBeans( type, Query.where( Require.nothing() ) ).toSet() );
                LOG.info( "Recorded " + m_oldDatabase.get( type ).size() 
                          + " old database records for " + type.getName() + "." );
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Could not record old database records for " + type.getName() + ".", ex );
            }
        }

        final Set< String > schemas = new HashSet<>();
        try
        {
            final Statement statement = m_connection.createStatement();
            final ResultSet rs = 
                    statement.executeQuery( "select schema_name from information_schema.schemata" );
            while ( rs.next() )
            {
                final String schema = rs.getString( 1 );
                if ( schema.startsWith( "pg_" ) || schema.equals( "public" ) 
                        || schema.equals( "information_schema" ) )
                {
                    continue;
                }
                schemas.add( schema );
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to determine db schemas.", ex );
        }
        
        for ( final String schema : schemas )
        {
            try
            {
                final Statement statement = m_connection.createStatement();
                statement.execute( "drop schema " + schema + " cascade;" );
                LOG.info( "Nuked schema " + schema + "." );
            }
            catch ( final Exception ex )
            {
                LOG.error( "Failed to nuke schema " + schema + ".", ex );
            }
        }
    }
    
    
    private void createDatabase()
    {
        LOG.info( "Creating database..." );
        
        final String sql = new SqlCodeGenerator( m_dataManager.getSupportedTypes(), "Administrator" )
            .getGeneratedCode().getCodeFiles().get( null );

        try
        {
            final Statement statement = m_connection.createStatement();
            statement.execute( sql );
            LOG.info( "Created database." );
        }
        catch ( final Exception ex )
        {
            LOG.error( "Failed to create database using script: " + Platform.NEWLINE + sql, ex );
        }

        LOG.info( "Restoring old database data..." );
        while ( restoreDatabaseData( false ) )
        {
            LOG.info( "The completed restore iteration made progress.  Will continue restore." );
        }
        restoreDatabaseData( true );
        
        if ( m_oldDatabase.isEmpty() )
        {
            LOG.info( LogUtil.getLogMessageImportantHeaderBlock( "Full restore successful." ) );
        }
        else
        {
            LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Partial restore successful." ) );
            LOG.warn( "Everything was restored except: " + m_oldDatabase );
        }
    }
    
    
    private boolean restoreDatabaseData( final boolean logExceptions )
    {
        LOG.info( "Restore iteration working..." );
        final AtomicBoolean atLeastOneSuccess = new AtomicBoolean( false );
        final Set< Future< ? > > futures = new HashSet<>();
        for ( final Map.Entry< Class< ? >, Set< ? extends DatabasePersistable > > e 
                : new HashSet<>( m_oldDatabase.entrySet() ) )
        {
            futures.add( SystemWorkPool.getInstance().submit( new Runnable()
            {
                public void run()
                {
                    for ( final DatabasePersistable bean : new ArrayList<>( e.getValue() ) )
                    {
                        try
                        {
                            m_dataManager.createBean( bean );
                            m_oldDatabase.get( e.getKey() ).remove( bean );
                            atLeastOneSuccess.set( true );
                        }
                        catch ( final Exception ex )
                        {
                            if ( logExceptions )
                            {
                                LOG.warn( "Failed to restore " + bean + ".", ex );
                            }
                        }
                    }
                    final Set< ? extends DatabasePersistable > s =
                                                m_oldDatabase.get( e.getKey() );
                    if ( null == s || s.isEmpty() )
                    {
                        m_oldDatabase.remove( e.getKey() );
                    }
                }
            } ) );
        }
        
        for ( final Future< ? > future : futures )
        {
            try
            {
                future.get();
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }

        LOG.info( "Restore iteration completed." );
        return atLeastOneSuccess.get();
    }
    

    private DataManager m_dataManager;
    private final Connection m_connection;
    private final Set< Class< ? extends DatabasePersistable > > m_dataTypes;
    private final static Logger LOG = Logger.getLogger( DataSourceNuker.class );
    private final Map< Class< ? >, Set< ? extends DatabasePersistable > > m_oldDatabase = new HashMap<>();
    
    public final static String ENABLE_NUKER_FLAG = "nukedatasource";
}
