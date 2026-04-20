/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.test.TestConnectionPool;
import com.spectralogic.util.lang.Validations;

/** */
public final class ConnectionPoolFactory
{

    private ConnectionPoolFactory()
    {
        // Intentionally empty.
    }

    
    public static ConnectionPoolFactory getInstance()
    {
        return CONNECTION_POOL_FACTORY;
    }
    
    
    public ConnectionPool getPool( final DataSource dataSource,
                                   final boolean defaultAutoCommitValue, 
                                   final int defaultTransactionIsolationLevel,
                                   final int maxNumberOfConnections,
                                   final int timeoutGettingConnectionInMillis )
    {
        Validations.verifyNotNull( "dataSoure", dataSource );
        
        if( USE_TEST_CONN_POOL.get() )
        {
            LOG.info( "Returning a testing connection pool." );
            return new TestConnectionPool( dataSource, defaultAutoCommitValue, 
                     defaultTransactionIsolationLevel, maxNumberOfConnections,
                                             timeoutGettingConnectionInMillis );
        }
        
        LOG.info( "Returning a default (production) connection pool." );
        return new DefaultConnectionPool( dataSource, defaultAutoCommitValue, 
                 defaultTransactionIsolationLevel, maxNumberOfConnections,
                                         timeoutGettingConnectionInMillis );
    }
    
    
    private static final ConnectionPoolFactory CONNECTION_POOL_FACTORY =
                                                    new ConnectionPoolFactory();
    
    private static final Logger LOG = Logger.getLogger( ConnectionPoolFactory.class );
    
    /**
     * Update this constant ONLY during test runs. NEVER access or change
     * the value of this constant in any non-test run context.
     */
    private final static AtomicBoolean USE_TEST_CONN_POOL =
                                                    new AtomicBoolean( false );
    public  final static String  USE_TEST_CONN_POOL_FIELD = "USE_TEST_CONN_POOL";
}
