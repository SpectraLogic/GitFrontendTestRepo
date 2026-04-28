/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.StandardShutdownListener;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;

public final class DefaultConnectionPool extends BaseShutdownable implements ConnectionPool
{
    public DefaultConnectionPool( 
            final DataSource dataSource,
            final boolean defaultAutoCommitValue, 
            final int defaultTransactionIsolationLevel,
            final int maxNumberOfConnections,
            final int timeoutGettingConnectionInMillis )
    {
        m_dataSource = dataSource;
        m_autoCommit = defaultAutoCommitValue;
        m_transactionIsolationLevel = defaultTransactionIsolationLevel;
        m_maxConnections = maxNumberOfConnections;
        m_timeoutGettingConnectionInMillis = timeoutGettingConnectionInMillis;
        Validations.verifyNotNull( "Data source", m_dataSource );
        
        if ( Integer.MAX_VALUE == m_maxConnections )
        {
            m_connectionReservationQueue = null;
        }
        else
        {
            m_connectionReservationQueue = new ArrayBlockingQueue<>( m_maxConnections, true );
            for ( int i = 0; i < m_maxConnections; ++i )
            {
                m_connectionReservationQueue.offer( new Object() );
            }
        }
        
        addShutdownListener( new CleanupOnShutdown() );
    }
    
    
    private final class CleanupOnShutdown extends StandardShutdownListener
    {
        public void shutdownOccurred()
        {
            synchronized ( DefaultConnectionPool.this )
            {
                int count = 0;
                int errors = 0;
                for ( final Connection c : m_connections.keySet() )
                {
                    try
                    {
                        ++count;
                        c.close();
                    }
                    catch ( final SQLException ex )
                    {
                        ++errors;
                        LOG.warn( "Failed to close connection.", ex );
                    }
                }
                
                LOG.info( count + " SQL connections needed to be closed.  "
                       + ( count - errors ) + " SQL connections were closed without error." );
            }
        }
    } // end inner class def
    
    
    public Connection takeConnection()
    {
        final Set< Connection > reservedConnections = m_reservedConnections.get( getThreadKey() );
        if ( null != reservedConnections )
        {
            if ( reservedConnections.isEmpty() )
            {
                throw new IllegalStateException( "Reservation of SQL connections was insufficient." );
            }
            final Connection retval = reservedConnections.iterator().next();
            reservedConnections.remove( retval );
            return retval;
        }
        
        return takeConnection( 20, m_timeoutGettingConnectionInMillis );
    }
    
    
    private Connection takeConnection(
            int sleepTimeBetweenRetries,
            final int timeoutGettingConnectionInMillis )
    {
        Connection retval = null;
        final long deadlineMillis =
                System.currentTimeMillis() + timeoutGettingConnectionInMillis;
        final MonitoredWork work = new MonitoredWork(
                StackTraceLogging.NONE, "Acquire SQL connection (max is " + m_maxConnections + ")" );
        try
        {
            while ( null == retval )
            {
                try
                {
                    if ( null != m_connectionReservationQueue )
                    {
                        final long remaining = deadlineMillis - System.currentTimeMillis();
                        if ( 0 >= remaining
                                || null == m_connectionReservationQueue.poll(
                                        remaining, TimeUnit.MILLISECONDS ) )
                        {
                            throw new RuntimeException(
                                    "Failed to acquire a SQL connection within "
                                            + timeoutGettingConnectionInMillis + "ms." );
                        }
                    }

                    retval = takeConnectionInternal();
                    if ( null == retval )
                    {
                        // establishConnection() failed; return the ticket we polled above
                        // so a transient DB outage doesn't permanently shrink the pool.
                        // The overall deadline above bounds total retry time.
                        if ( null != m_connectionReservationQueue )
                        {
                            m_connectionReservationQueue.add( new Object() );
                        }
                        Thread.sleep( sleepTimeBetweenRetries );
                        sleepTimeBetweenRetries += 50;
                    }
                    else if ( !retval.isValid( 0 ) )
                    {
                        retval.close();
                        returnConnection( retval );
                        retval = null;
                    }
    
                    if ( null != retval )
                    {
                        try
                        {
                            if ( m_autoCommit != retval.getAutoCommit() )
                            {
                                retval.setAutoCommit( m_autoCommit );
                            }
                            if ( m_transactionIsolationLevel != retval.getTransactionIsolation() )
                            {
                                retval.setTransactionIsolation( m_transactionIsolationLevel );
                            }
                        }
                        catch ( final SQLException ex )
                        {
                            throw new RuntimeException( ex );
                        }
                    }
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
                catch ( final SQLException ex )
                {
                    throw new DaoException( "Failure while attempting to take a SQL connection.", ex );
                }
            }
            
            return retval;
        }
        finally
        {
            work.completed();
        }
    }
    
    
    synchronized private Connection takeConnectionInternal() throws SQLException
    {
        verifyNotShutdown();
        
        for ( final Map.Entry< Connection, MonitoredWork > e : new HashSet<>( m_connections.entrySet() ) )
        {
            if ( null != e.getValue() )
            {
                continue;
            }
            
            if ( e.getKey().isClosed() )
            {
                m_connections.remove( e.getKey() );
                continue;
            }
            m_connections.put( e.getKey(), createMonitoredWork() );
            return e.getKey();
        }
        
        final Connection c;
        try
        {
            c = m_dataSource.establishConnection();
        }
        catch ( final Exception ex )
        {
            LOG.error( "Failed to establish SQL connection.", ex );
            return null;
        }
        m_connections.put( c, createMonitoredWork() );
        return c;
    }
    
    
    private MonitoredWork createMonitoredWork()
    {
        return new MonitoredWork( StackTraceLogging.FULL, "SQL connection in use (checked out of pool)." )
            .withCustomLogger( DbLogger.DB_HOG_LOG );
    }
    
    
    public synchronized void returnConnection( final Connection connection )
    {
        final Set< Connection > reservedConnections = m_reservedConnections.get( getThreadKey() );
        if ( null != reservedConnections )
        {
            reservedConnections.add( connection );
            return;
        }
        
        if ( !m_connections.containsKey( connection ) )
        {
            throw new IllegalStateException( "SQL connection unknown: " + connection );
        }
        
        final MonitoredWork work = m_connections.get( connection );
        if ( null == work )
        {
            throw new IllegalStateException( "SQL connection not checked out of pool: " + connection );
        }
        work.completed();
        m_connections.put( connection, null );
        
        if ( null != m_connectionReservationQueue )
        {
            m_connectionReservationQueue.add( new Object() );
        }
    }
    
    
    public void reserveConnections( final int numberOfConnections )
    {
        if ( null == m_connectionReservationQueue )
        {
            throw new IllegalStateException(
                    "Cannot reserve connections on an unlimited connection queue." );
        }
    	reserveConnections( numberOfConnections, 90000 );
    }
    
    
    void reserveConnections( final int numberOfConnections, final int timeoutInMillis )
    {
        m_reservationLock.writeLock().lock();
        try
        {
            reserveConnectionsInternal( numberOfConnections, timeoutInMillis );
        }
        finally
        {
            m_reservationLock.writeLock().unlock();
        }
    }
    
    
    private void reserveConnectionsInternal( final int numberOfConnections, final int timeoutInMillis )
    {
        synchronized ( this )
        {
            if ( m_reservedConnections.containsKey( getThreadKey() ) )
            {
                throw new IllegalStateException( 
                        "SQL connections already reserved.  Cannot reserve more now." );
            }
        }
        
        final Set< Connection > connections = new HashSet<>();
        if ( 0 < numberOfConnections )
        {
            Validations.verifyInRange(
                    "Number of SQL connections", 1, m_maxConnections, numberOfConnections );
            final Duration duration = new Duration();
            while ( connections.isEmpty() )
            {
                try
                {
                    for ( int i = 0; i < numberOfConnections; ++i )
                    {
                        connections.add( takeConnection( 20, 250 ) );
                    }
                }
                catch ( final RuntimeException ex )
                {
                    if ( timeoutInMillis < duration.getElapsedMillis() )
                    {
                        releaseReservedConnections( connections );
                        throw new RuntimeException(
                                "Failed to reserve " + numberOfConnections + " SQL connections within " 
                                + timeoutInMillis + "ms.", ex );
                        
                    }
                    releaseReservedConnections( connections );
                    connections.clear();
                    
                    try
                    {
                        Thread.sleep( 50 );
                    }
                    catch ( final InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }

        synchronized ( this )
        {
            m_reservedConnections.put( getThreadKey(), connections );
            m_reservedConnectionCounts.put( getThreadKey(), Integer.valueOf( numberOfConnections ) );
            return;
        }
    }
    
    
    public void releaseReservedConnections()
    {
        synchronized ( this )
        {
            if ( !m_reservedConnections.containsKey( getThreadKey() ) )
            {
                throw new IllegalStateException( "No SQL connections were reserved on thread." );
            }
            final int expectedConnectionCount = m_reservedConnectionCounts.get( getThreadKey() ).intValue();
            final int actualConnectionCount = m_reservedConnections.get( getThreadKey() ).size();
            if ( expectedConnectionCount != actualConnectionCount )
            {
                throw new IllegalStateException( 
                        "Expected " + expectedConnectionCount
                        + " SQL connections to have been returned, but only " + actualConnectionCount 
                        + " SQL connections have been returned." );
            }
            
            m_reservedConnectionCounts.remove( getThreadKey() );
            releaseReservedConnections( m_reservedConnections.remove( getThreadKey() ) );
        }
    }
    
    
    private void releaseReservedConnections( final Set< Connection > connections )
    {
        for ( final Connection c : connections )
        {
            returnConnection( c );
        }
    }
    
    
    private Long getThreadKey()
    {
        return Long.valueOf( Thread.currentThread().getId() );
    }
    
    
    private final DataSource m_dataSource;
    private final Map< Connection, MonitoredWork > m_connections = new HashMap<>();
    private final boolean m_autoCommit;
    private final int m_transactionIsolationLevel;
    private final int m_maxConnections;
    private final int m_timeoutGettingConnectionInMillis;
    private final BlockingQueue< Object > m_connectionReservationQueue;
    private final ReadWriteLock m_reservationLock = new ReentrantReadWriteLock( true );
    private final Map< Long, Set< Connection > > m_reservedConnections = new HashMap<>();
    private final Map< Long, Integer > m_reservedConnectionCounts = new HashMap<>();
    
    private final static Logger LOG = Logger.getLogger( DefaultConnectionPool.class );
}
