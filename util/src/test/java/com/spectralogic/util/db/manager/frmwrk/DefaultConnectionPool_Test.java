/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.postgres.PostgresDataSource;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class DefaultConnectionPool_Test 
{
    @Test
    public void testConstructorDataSourceNullNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DefaultConnectionPool( 
                        null,
                        true, 
                        Connection.TRANSACTION_READ_COMMITTED,
                        1,
                        1 );
            }
        } );
    }
    
    
    @Test
    public void testConnectionsArePooledWhenPoolBounded() throws InterruptedException, SQLException
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        try
        {
            final int numberOfConnections = 4;
            final int numberOfWorkers = 8;
            final ConnectionPool pool = new DefaultConnectionPool( 
                    new PostgresDataSource( dbSupport.getDbServerName(),
                                            dbSupport.getDbName(),
                                            dbSupport.getDbUsername(),
                                            dbSupport.getDbPassword() ),
                    true, 
                    Connection.TRANSACTION_READ_COMMITTED,
                    numberOfConnections,
                    5000 );
            final WorkPool wp = WorkPoolFactory.createWorkPool( numberOfWorkers, getClass().getSimpleName() );
            
            final CountDownLatch latch = new CountDownLatch( numberOfWorkers );
            final Set< Connection > connections = Collections.synchronizedSet( new HashSet< Connection >() );
            for ( int i = 0; i < numberOfWorkers; ++i )
            {
                wp.submit( new Runnable()
                {
                    public void run()
                    {
                        TestUtil.sleep( 1 );
                        final Connection c = pool.takeConnection();
                        connections.add( c );
                        TestUtil.sleep( 10 );
                        pool.returnConnection( c );
                        latch.countDown();
                    }
                } );
            }
            
            latch.await();
            
            assertEquals(
                    numberOfConnections,
                    connections.size(),
                    "Should never have had more than " + numberOfConnections + " connections."
                    );
            for ( final Connection c : connections )
            {
                assertFalse(
                        c.isClosed(),
                        "Connection should notta been closed down.");
            }
            
            pool.shutdown();
            for ( final Connection c : connections )
            {
                assertTrue(
                        c.isClosed(),
                        "Connection shoulda been closed down.");
            }
        }
        finally
        {
            dbSupport.getDataManager().shutdown();
        }
    }
    
    
    @Test
    public void testConnectionsArePooledWhenPoolUnbounded() throws InterruptedException, SQLException
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        try
        {
            final int numberOfWorkers = 8;
            final ConnectionPool pool = new DefaultConnectionPool( 
                    new PostgresDataSource( dbSupport.getDbServerName(),
                                            dbSupport.getDbName(),
                                            dbSupport.getDbUsername(),
                                            dbSupport.getDbPassword() ),
                    true, 
                    Connection.TRANSACTION_READ_COMMITTED,
                    Integer.MAX_VALUE,
                    5000 );
            final WorkPool wp = WorkPoolFactory.createWorkPool( numberOfWorkers, getClass().getSimpleName() );
            
            final CountDownLatch latch = new CountDownLatch( numberOfWorkers );
            final Set< Connection > connections = Collections.synchronizedSet( new HashSet< Connection >() );
            for ( int i = 0; i < numberOfWorkers; ++i )
            {
                wp.submit( new Runnable()
                {
                    public void run()
                    {
                        final Connection c = pool.takeConnection();
                        connections.add( c );
                        TestUtil.sleep( 40 );
                        pool.returnConnection( c );
                        latch.countDown();
                    }
                } );
                TestUtil.sleep( 10 );
            }
            
            latch.await();

            assertTrue(
                    connections.size() > 1,
                    "Shoulda created multiple connections when necessary.");
            assertTrue(
                    connections.size() < numberOfWorkers,
                    "Shoulda reused connections when available.");
            for ( final Connection c : connections )
            {
                assertFalse(
                        c.isClosed(),
                        "Connection should notta been closed down.");
            }
            
            pool.shutdown();
            for ( final Connection c : connections )
            {
                assertTrue(
                        c.isClosed(),
                        "Connection shoulda been closed down.");
            }
        }
        finally
        {
            dbSupport.getDataManager().shutdown();
        }
    }
    
    
    @Test
    public void testUnboundedPoolsDontCroakOnceAllSqlConnectionsGetUsedUp() 
            throws InterruptedException, SQLException
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        try
        {
            final int numberOfWorkers = 100;
            final ConnectionPool pool = new DefaultConnectionPool( 
                    new PostgresDataSource( dbSupport.getDbServerName(),
                                            dbSupport.getDbName(),
                                            dbSupport.getDbUsername(),
                                            dbSupport.getDbPassword() ),
                    true, 
                    Connection.TRANSACTION_READ_COMMITTED,
                    Integer.MAX_VALUE,
                    5000 );
            final WorkPool wp = WorkPoolFactory.createWorkPool( numberOfWorkers, getClass().getSimpleName() );
            
            final CountDownLatch latch = new CountDownLatch( numberOfWorkers );
            final Set< Connection > connections = Collections.synchronizedSet( new HashSet< Connection >() );
            for ( int i = 0; i < numberOfWorkers; ++i )
            {
                wp.submit( new Runnable()
                {
                    public void run()
                    {
                        TestUtil.sleep( 1 );
                        final Connection c = pool.takeConnection();
                        connections.add( c );
                        TestUtil.sleep( 10 );
                        pool.returnConnection( c );
                        latch.countDown();
                    }
                } );
            }
            
            latch.await();
            
            for ( final Connection c : connections )
            {
                assertFalse(
                        c.isClosed(),
                        "Connection should notta been closed down.");
            }
            
            pool.shutdown();
            for ( final Connection c : connections )
            {
                assertTrue(
                        c.isClosed(),
                        "Connection shoulda been closed down.");
            }
        }
       finally     
       {
           dbSupport.getDataManager().shutdown();
       }
    }
    
    
    @Test
    public void testReservationsWorkWhenPoolBounded() throws InterruptedException, SQLException
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        try
        {
            final int numberOfConnections = 3;
            final int numberOfWorkers = 3;
            final ConnectionPool pool = new DefaultConnectionPool( 
                    new PostgresDataSource( dbSupport.getDbServerName(),
                                            dbSupport.getDbName(),
                                            dbSupport.getDbUsername(),
                                            dbSupport.getDbPassword() ),
                    true, 
                    Connection.TRANSACTION_READ_COMMITTED,
                    numberOfConnections,
                    5000 );
            final WorkPool wp = WorkPoolFactory.createWorkPool( numberOfWorkers, getClass().getSimpleName() );
            
            final CountDownLatch latch = new CountDownLatch( numberOfWorkers );
            final Set< Connection > connections = Collections.synchronizedSet( new HashSet< Connection >() );
            for ( int i = 0; i < numberOfWorkers; ++i )
            {
                final int noc = i;
                wp.submit( new Runnable()
                {
                    public void run()
                    {
                        TestUtil.sleep( 1 );
                        
                        for ( int j = 0; j < 3; ++j )
                        {
                            final Set< Connection > myConnections = new HashSet<>();
                            pool.reserveConnections( noc );
                            for ( int k = 0; k < noc; ++k )
                            {
                                final Connection c = pool.takeConnection();
                                myConnections.add( c );
                                connections.add( c );
                                if ( 1 == noc && 1 == k )
                                {
                                    pool.returnConnection( c );
                                    pool.takeConnection();
                                }
                            }
                            TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
                            {
                                public void test()
                                {
                                    pool.takeConnection();
                                }
                                } );
                            TestUtil.sleep( 10 + new SecureRandom().nextInt( 10 ) );
                            if ( 0 < noc )
                            {
                                TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
                                {
                                    public void test()
                                    {
                                        pool.releaseReservedConnections();
                                    }
                                } );
                            }
                            for ( final Connection c : myConnections )
                            {
                                pool.returnConnection( c );
                            }
                            pool.releaseReservedConnections();
                            TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
                            {
                                public void test()
                                {
                                    pool.releaseReservedConnections();
                                }
                            } );
                        }
                        
                        latch.countDown();
                    }
                } );
            }
            
            latch.await();
            
            assertEquals(
                    numberOfConnections,
                    connections.size(),
                    "Should never have had more than " + numberOfConnections + " connections."
                    );
            for ( final Connection c : connections )
            {
                assertFalse(
                        c.isClosed(),
                        "Connection should notta been closed down.");
            }
            
            pool.shutdown();
            for ( final Connection c : connections )
            {
                assertTrue(
                        c.isClosed(),
                        "Connection shoulda been closed down.");
            }
        }
        finally
        {
            dbSupport.getDataManager().shutdown();
        }
    }
    
    
    @Test
    public void testReservationCannotBeMadeWhenNotPossibleToMakeReservation() 
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        try
        {
            final DefaultConnectionPool pool = new DefaultConnectionPool( 
                    dbSupport.getDataSource(), 
                    true, 
                    Connection.TRANSACTION_READ_COMMITTED,
                    2,
                    5000 );
            
            pool.reserveConnections( 1 );
            TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
            {
                public void test()
                {
                    pool.reserveConnections( 1 );
                }
            } );
            
            pool.releaseReservedConnections();
            pool.reserveConnections( 2 );
            pool.releaseReservedConnections();
            
            SystemWorkPool.getInstance().submit( new Runnable()
            {
                public void run()
                {
                    final Connection c = pool.takeConnection();
                    TestUtil.sleep( 500 );
                    pool.returnConnection( c );
                }
            } );
            
            pool.reserveConnections( 2 );
            pool.releaseReservedConnections();
    
            final Connection c = pool.takeConnection();
            TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
            {
                public void test()
                {
                    pool.reserveConnections( 2, 200 );
                }
            } );
            pool.returnConnection( c );
            
            pool.reserveConnections( 2 );
            pool.releaseReservedConnections();
        }
        finally
        {
            dbSupport.getDataManager().shutdown();
        }
    }
    
    
    @Test
    public void testTicketsAreNotLeakedWhenEstablishConnectionFails()
    {
        // DataSource that throws a few times (transient outage) then heals.
        final AtomicInteger failuresRemaining = new AtomicInteger( 2 );
        final DataSource ds = () -> {
            if ( failuresRemaining.getAndDecrement() > 0 )
            {
                throw new RuntimeException( "simulated Postgres outage" );
            }
            return newFakeConnection();
        };

        final int poolSize = 3;
        final int pollTimeoutMs = 200;
        final DefaultConnectionPool pool = new DefaultConnectionPool(
                ds, true, Connection.TRANSACTION_READ_COMMITTED, poolSize, pollTimeoutMs );

        // All three tickets should be usable. The retry-loop inside
        // the first takeConnection() would have drained extra tickets while
        // absorbing the 2 failures — a later call would then time out.
        final Connection c1 = pool.takeConnection();
        final Connection c2 = pool.takeConnection();
        final Connection c3 = pool.takeConnection();
        assertNotNull( c1 );
        assertNotNull( c2 );
        assertNotNull( c3 );

        pool.returnConnection( c1 );
        pool.returnConnection( c2 );
        pool.returnConnection( c3 );
        pool.shutdown();
    }


    @Test
    public void testRetryLoopDoesNotCompoundTicketLeak()
    {
        // DataSource that fails until healed via the flag.
        final AtomicBoolean broken = new AtomicBoolean( true );
        final DataSource ds = () -> {
            if ( broken.get() )
            {
                throw new RuntimeException( "simulated Postgres outage" );
            }
            return newFakeConnection();
        };

        final int poolSize = 10;
        final int pollTimeoutMs = 300;
        final DefaultConnectionPool pool = new DefaultConnectionPool(
                ds, true, Connection.TRANSACTION_READ_COMMITTED, poolSize, pollTimeoutMs );

        // Force one failed takeConnection() to completion. With Bug #2, its
        // retry loop would drain many tickets before the overall timeout.
        try
        {
            pool.takeConnection();
            fail( "takeConnection() should have thrown — DataSource is broken." );
        }
        catch ( final RuntimeException expected )
        {
        }

        // Heal the DS. All poolSize tickets should still be available; with the
        // bug, some subset of these takes would time out because tickets leaked.
        broken.set( false );
        final Set< Connection > taken = new HashSet<>();
        for ( int i = 0; i < poolSize; ++i )
        {
            taken.add( pool.takeConnection() );
        }
        assertEquals( poolSize, taken.size() );

        for ( final Connection c : taken )
        {
            pool.returnConnection( c );
        }
        pool.shutdown();
    }


    private static Connection newFakeConnection()
    {
        final InvocationHandler h = new InvocationHandler()
        {
            public Object invoke( final Object proxy, final java.lang.reflect.Method method,
                                  final Object[] args )
            {
                final String name = method.getName();
                switch ( name )
                {
                    case "isValid":                 return Boolean.TRUE;
                    case "isClosed":                return Boolean.FALSE;
                    case "getAutoCommit":           return Boolean.TRUE;
                    case "getTransactionIsolation": return Integer.valueOf( Connection.TRANSACTION_READ_COMMITTED );
                    case "setAutoCommit":
                    case "setTransactionIsolation":
                    case "close":                   return null;
                    case "equals":                  return Boolean.valueOf( proxy == args[0] );
                    case "hashCode":                return Integer.valueOf( System.identityHashCode( proxy ) );
                    case "toString":                return "FakeConn@" + System.identityHashCode( proxy );
                    default: throw new UnsupportedOperationException( name );
                }
            }
        };
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] { Connection.class },
                h );
    }


    @Test
    public void testReservationsNotAllowedWhenPoolUnbounded()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        try
        {
            final ConnectionPool pool = new DefaultConnectionPool( 
                    dbSupport.getDataSource(), 
                    true, 
                    Connection.TRANSACTION_READ_COMMITTED,
                    Integer.MAX_VALUE,
                    5000 );
            TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
            {
                public void test()
                {
                    pool.reserveConnections( 1 );
                }
            } );
        }
        finally
        {
            dbSupport.getDataManager().shutdown();
        }
    }
}
