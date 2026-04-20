/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.test;


import java.sql.Connection;

import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.lang.Validations;


public final class TestDataSource implements DataSource
{
    
    public TestDataSource( 
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
        return DatabaseConnectionCache.establishDbConnection( m_userName,
                                                 m_password, m_connectionUrl );
    }
    
    
    private final String m_userName;
    private final String m_password;
    private final String m_connectionUrl;
}
