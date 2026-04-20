/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.spectraview;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Level;

import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class SpectraViewRestRequest_Test 
{
    @Test
    public void testConstructorNullRequestTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new SpectraViewRestRequest( null, "", Level.INFO );
                }
            } );
    }
    
    
    @Test
    public void testConstructorNullPathNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new SpectraViewRestRequest( RequestType.values()[ 0 ], null, Level.INFO );
                }
            } );
    }
    
    
    @Test
    public void testHappyConstruction()
    {
        new SpectraViewRestRequest( RequestType.values()[ 0 ], "", Level.INFO );
    }
}
