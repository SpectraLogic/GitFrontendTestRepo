/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.frmwrk.ConnectionPool;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.db.test.TestConnectionPool;
import com.spectralogic.util.db.test.TestDataSource;
import com.spectralogic.util.lang.NamingConventionType;







/** */
public class TestConnectionPool_Test 
{
    @Test
    public void testNullDataSourceNotAllowed()
    {
        TestUtil.assertThrows(
                 "Null data source shouldn't have been allowed.",
                 IllegalArgumentException.class,
                 new TestUtil.BlastContainer()
                 {
                    public void test() throws Throwable
                    {
                        new TestConnectionPool( null, true, 0, 0, 0 );
                    }
                 } );
    }
    
    
    @Test
    public void testConnectionAutoCommitValuesAreAsRequested()
    {
        final DatabaseSupport dbSupport = 
                 DatabaseSupportFactory.getSupport(
                                         Teacher.class, TeacherService.class );
        final DataSource dataSource = new TestDataSource(
                    "localhost", dbSupport.getDbName(), DB_USER, DB_PASSWORD );
        
        ConnectionPool cp = new TestConnectionPool(
                dataSource, true, Connection.TRANSACTION_READ_UNCOMMITTED, 3,
                15 * 60 * 1000 );
        
        Connection c = cp.takeConnection();
        try
        {
            assertTrue(
                    c.getAutoCommit(),
                    "Autocommit value on Connection shoulda been true."
            );
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
        cp.returnConnection( c );
        cp.shutdown();
        
        
        cp = new TestConnectionPool(
                dataSource, false, Connection.TRANSACTION_READ_UNCOMMITTED, 3,
                15 * 60 * 1000 );
        
        c = cp.takeConnection();
        try
        {
            assertFalse(
                    c.getAutoCommit(),
                    "Autocommit value on Connection shoulda been false."
            );
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
        cp.returnConnection( c );
        cp.shutdown();
    }
    
    
    @Test
    public void testConnectionTransactionLevelValuesAreAsRequested()
    {
        final DatabaseSupport dbSupport = 
                 DatabaseSupportFactory.getSupport(
                                         Teacher.class, TeacherService.class );
        final DataSource dataSource = new TestDataSource(
                    "localhost", dbSupport.getDbName(), DB_USER, DB_PASSWORD );
        
        ConnectionPool cp = new TestConnectionPool(
                dataSource, true, Connection.TRANSACTION_READ_UNCOMMITTED, 3,
                15 * 60 * 1000 );
        
        Connection c = cp.takeConnection();
        try
        {
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED,  c.getTransactionIsolation(), "Trans level on Connection shoulda been what was requested (1).");
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
        cp.returnConnection( c );
        cp.shutdown();
        
       
        cp = new TestConnectionPool(
               dataSource, true, Connection.TRANSACTION_READ_COMMITTED, 3,
               15 * 60 * 1000 );
       
        c = cp.takeConnection();
        try
        {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED,  c.getTransactionIsolation(), "Trans level on Connection shoulda been what was requested (2).");
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
        cp.returnConnection( c );
        cp.shutdown();
       
       
        cp = new TestConnectionPool(
                dataSource, true, Connection.TRANSACTION_REPEATABLE_READ, 3,
                15 * 60 * 1000 );
        
        c = cp.takeConnection();
        try
        {
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ,  c.getTransactionIsolation(), "Trans level on Connection shoulda been what was requested (3).");
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
        cp.returnConnection( c );
        cp.shutdown();
    }

    
    private final static String DB_USER =
             NamingConventionType.UNDERSCORED.convert( 
                DatabaseSupportFactory.class.getSimpleName() + "DatabaseUser" );
    
    private final static String DB_PASSWORD = "passw0rd";
}
