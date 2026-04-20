/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.postgres;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.frmwrk.DbLogger;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Validations;

public final class PostgresDataSource implements DataSource
{
    public PostgresDataSource( 
            final String serverName, 
            final String databaseName,
            final String userName,
            final String password )
    {
        m_userName = userName;
        m_password = password;
        
        Validations.verifyNotNull( "Server name", serverName );
        Validations.verifyNotNull( "Database name", databaseName );
        Validations.verifyNotNull( "User name", m_userName );
        
        m_connectionUrl = "jdbc:postgresql://" + serverName + "/" + databaseName;
    }
    
    
    public Connection establishConnection()
    {
        try
        {
            Class.forName( "org.postgresql.Driver" ); // ensure Postgres driver is loaded for DriverManager
            final Connection retval = DriverManager.getConnection( m_connectionUrl, m_userName, m_password );
            registerNewConnection( retval );
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to establish a connection to " + m_connectionUrl, ex );
        }
    }
    
    
    private void registerNewConnection( final Connection connection )
    {
        synchronized ( CONNECTIONS )
        {
            for ( final WeakReference< Connection > wr : new HashSet<>( CONNECTIONS ) )
            {
                if ( null == wr.get() )
                {
                    CONNECTIONS.remove( wr );
                }
            }
            
            CONNECTIONS.add( new WeakReference<>( connection ) );
            
            DbLogger.DB_HOG_LOG.info( "Established a new SQL connection with the database ("
                    + CONNECTIONS.size() + " connections total):" 
                    + ExceptionUtil.getLimitedStackTrace( Thread.currentThread().getStackTrace(), 24 ) );
        }
    }
    
    
    private final String m_userName;
    private final String m_password;
    private final String m_connectionUrl;
    
    private final static Set< WeakReference< Connection > > CONNECTIONS = new HashSet<>();
}
