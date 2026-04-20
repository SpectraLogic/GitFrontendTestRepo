/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.CriticalShutdownListener;

public class SingleConnectionConnectionPool extends BaseShutdownable implements ConnectionPool
{
    public SingleConnectionConnectionPool( final ConnectionPool connectionPool )
    {
        Validations.verifyNotNull( "Connection pool", connectionPool );
        m_pool = connectionPool;
        m_connection = connectionPool.takeConnection();
        
        addShutdownListener( new CriticalShutdownListener()
        {
            public void shutdownOccurred()
            {
                m_pool.returnConnection( m_connection );
            }
        } );
        doNotLogWhenShutdown();
    }

    
    synchronized public Connection takeConnection()
    {
        if ( null != m_reservationLatch )
        {
            try
            {
                m_reservationLatch.await();
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        m_reservationLatch = new CountDownLatch( 1 );
        validateConnection();
        return m_connection;
    }
    
    
    private void validateConnection()
    {
        try
        {
            if ( !m_connection.isClosed() )
            {
                return;
            }
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( "Failed to check if connection was closed.", ex );
        }
        
        LOG.warn( "Connection was not valid.  Will take a new connection." );
        m_pool.returnConnection( m_connection );
        m_connection = m_pool.takeConnection();
    }

    
    public void returnConnection( final Connection connection )
    {
        if ( connection != m_connection )
        {
            throw new IllegalArgumentException( "Connection does not belong to pool: " + connection );
        }
        m_reservationLatch.countDown();
    }

    
    public void reserveConnections( int numberOfConnections )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public void releaseReservedConnections()
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    private volatile Connection m_connection;
    private volatile CountDownLatch m_reservationLatch;
    
    private final ConnectionPool m_pool;
    
    private final static Logger LOG = Logger.getLogger( SingleConnectionConnectionPool.class );
}
