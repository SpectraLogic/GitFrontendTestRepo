/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.manager.postgres.PostgresDataSource;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class SingleConnectionConnectionPool_Test 
{
    @Test
    public void testConstructorNullConnectionNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new SingleConnectionConnectionPool( null );
                }
            } );
    }
    
    
    @Test
    public void testConnectionIsManagedCorrectly() throws InterruptedException, SQLException
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final int numberOfConnections = 1;
        final int numberOfWorkers = 8;
        final ConnectionPool parentPool = new DefaultConnectionPool( 
                    new PostgresDataSource( dbSupport.getDbServerName(),
                                            dbSupport.getDbName(),
                                            dbSupport.getDbUsername(),
                                            dbSupport.getDbPassword() ),
                    false, Connection.TRANSACTION_SERIALIZABLE, 2, 1 );
        final ConnectionPool pool = new SingleConnectionConnectionPool( parentPool );
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
                    final Connection conn = pool.takeConnection();
                    connections.add( conn );
                    TestUtil.sleep( 10 );
                    pool.returnConnection( conn );
                    latch.countDown();
                }
            } );
        }
        
        latch.await();

        assertEquals(numberOfConnections,  connections.size(), "Should never have had more than " + numberOfConnections + " connections.");
        assertFalse(
                connections.iterator().next().isClosed(),
                "Connection should notta been closed down.");
        
        pool.shutdown();
        assertFalse(
                connections.iterator().next().isClosed(),
                "Connection should notta been closed down.");

        parentPool.shutdown();
        assertTrue(
                connections.iterator().next().isClosed(),
                "Connection shoulda been closed down.");
    }
    
    
    @Test
    public void testInvalidReturnConnectionThrowsIllegalArgumentException()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        final ConnectionPool parentPool = new DefaultConnectionPool( 
                dbSupport.getDataSource(), false, Connection.TRANSACTION_SERIALIZABLE, 1, 100 );
        final ConnectionPool pool = new SingleConnectionConnectionPool( parentPool );
        final WorkPool wp = WorkPoolFactory.createWorkPool( 1, getClass().getSimpleName() );

        wp.submit( new Runnable()
        {
            public void run()
            {
                final Connection conn = pool.takeConnection();
                pool.returnConnection( conn );
                
                TestUtil.assertThrows( 
                        "Should notta been able to return ConnectionPool connection twice.", 
                        IllegalArgumentException.class, 
                        new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        pool.returnConnection( conn );
                    }
                } );
                
            }
        } );
        
        pool.shutdown();
        parentPool.shutdown();
    }
    
    
    @Test
    public void testReserveConnectionsNotSupported()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
            
        final ConnectionPool parentPool = new DefaultConnectionPool( 
                dbSupport.getDataSource(), false, Connection.TRANSACTION_SERIALIZABLE, 1, 100 );
        final ConnectionPool pool = new SingleConnectionConnectionPool( parentPool );

        TestUtil.assertThrows( 
                null,
                UnsupportedOperationException.class, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        pool.reserveConnections( 1 );
                    }
                } );
    }
    
    
    @Test
    public void testReleaseReservedConnectionsNotSupported()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
            
        final ConnectionPool parentPool = new DefaultConnectionPool( 
                dbSupport.getDataSource(), false, Connection.TRANSACTION_SERIALIZABLE, 1, 100 );
        final ConnectionPool pool = new SingleConnectionConnectionPool( parentPool );

        TestUtil.assertThrows( 
                null,
                UnsupportedOperationException.class, 
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        pool.releaseReservedConnections();
                    }
                } );
    }
}
