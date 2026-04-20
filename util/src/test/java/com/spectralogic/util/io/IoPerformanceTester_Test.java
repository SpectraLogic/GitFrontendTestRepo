/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class IoPerformanceTester_Test
{
    @Test
    public void testConstructorNullDirectoryNotAllowed()
    {
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
                {
                    new IoPerformanceTester( null, 10 );
                }
            } );
    }
    
    
    @Test
    public void testConstructorInvalidDirectoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new IoPerformanceTester( "oopsies", 10 );
                }
            } );
    }
    
    
    @Test
    public void testRunEventuallyCompletes()
    {
        new IoPerformanceTester( System.getProperty( "java.io.tmpdir" ), 10 ).run();
    }
}
