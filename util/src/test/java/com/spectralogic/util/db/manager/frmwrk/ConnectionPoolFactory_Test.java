/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.db.test.TestConnectionPool;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

/** */
public class ConnectionPoolFactory_Test 
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
                       ConnectionPoolFactory.getInstance().getPool(
                                                         null, true, 0, 0, 0 );
                   }
                } ); 
    }
    
    
    @Test
    public void testThatItReturnsTestConnectionPoolsWhenDatabaseSupportIsInUse()
    {
         final DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(
                                          Teacher.class, TeacherService.class );
         assertTrue(
                 TestConnectionPool.class.isAssignableFrom(
                            ConnectionPoolFactory.getInstance().getPool(
                               dbSupport.getDataSource(), true, 0, 0, 0 ).getClass() ),
                 "Shoulda been able to assign to type " + TestConnectionPool.class);
    }

}
