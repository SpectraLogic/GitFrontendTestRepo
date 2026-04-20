/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.test;


import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;


public class DatabaseConnectionCache_Test 
{
    @Test
    public void testCloseConnectionDoesNotCloseConnection()
    {
        final DatabaseSupport dbSprt = DatabaseSupportFactory.getSupport(
                                          Teacher.class, TeacherService.class );
        final Connection c = DatabaseConnectionCache.establishDbConnection(
                                 dbSprt.getDbUsername(),
                                 dbSprt.getDbPassword(),
                              "jdbc:postgresql://" + dbSprt.getDbServerName() +
                                                     "/" + dbSprt.getDbName() );
        try
        {
            c.close();
            assertFalse(  // Yes, false--it should NOT be closed.
                    c.isClosed(), "DB connection shouldn't have been closed.");
        }
        catch ( final SQLException ex )
        {
            throw new RuntimeException( ex );
        }
    }
}
