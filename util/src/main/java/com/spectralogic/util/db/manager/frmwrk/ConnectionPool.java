/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.sql.Connection;

import com.spectralogic.util.shutdown.Shutdownable;

public interface ConnectionPool extends Shutdownable
{
    Connection takeConnection();
    
    
    void returnConnection( final Connection connection );
    
    
    void reserveConnections( final int numberOfConnections );
    
    
    void releaseReservedConnections();
}
