/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.test;


import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.frmwrk.ConnectionPool;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;


/**
 * Mimics DefaultConnectionPool.
 */
public final class TestConnectionPool extends BaseShutdownable
                                      implements ConnectionPool
{
    
    public TestConnectionPool(
            final DataSource dataSource,
            final boolean defaultAutoCommitValue, 
            final int defaultTransactionIsolationLevel,
            @SuppressWarnings( "unused" ) final int maxNumberOfConnections,
            @SuppressWarnings( "unused" ) final int timeoutGettingConnectionInMillis )
    {
        Validations.verifyNotNull( "dataSource", dataSource );
        try
        {
            // Do NOT remove this cast nor the error it logs.
            m_dataSource = (TestDataSource)dataSource;
        }
        catch( final ClassCastException e )
        {
            LOG.error( "Use this class only with a TestDataSource.", e );
            throw e;
        }
        m_defaultAutoCommit = defaultAutoCommitValue;
        m_defaultTransIsolationLevel = defaultTransactionIsolationLevel;
        
        // Not adding anything to the shut down pool is intentional.
    }
    
    
    @Override
    public Connection takeConnection()
    {
        final Connection c = m_dataSource.establishConnection();
        try
        {
            if( m_defaultAutoCommit != c.getAutoCommit() )
            {
                c.setAutoCommit( m_defaultAutoCommit );
            }
            if( m_defaultTransIsolationLevel != c.getTransactionIsolation() )
            {
                c.setTransactionIsolation( m_defaultTransIsolationLevel );
            }
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
        return c;
    }

    
    @Override
    public void returnConnection( final Connection c )
    {
        try
        {
            c.close(); // See TestDataSource to understand.
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    
    @Override
    public void reserveConnections( final int numberOfConnections )
    {
        // Add functionality only after this method not doing
        // anything causes at least one test method to fail.
    }

    
    @Override
    public void releaseReservedConnections()
    {
        // Add functionality only after this method not doing
        // anything causes at least one test method to fail.
    }
    
    
    private final TestDataSource m_dataSource;
    private final boolean m_defaultAutoCommit;
    private final int m_defaultTransIsolationLevel;
    
    private static final Logger LOG =
                                  Logger.getLogger( TestConnectionPool.class );
}
