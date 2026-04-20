/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class Shutdownable_Test 
{
    @Test
    public void testVerifyNotShutdownThrowsExceptionIffAlreadyShutdown()
    {
        final ConcreteShutdownable support = new ConcreteShutdownable();
        support.verifyNotShutdown();
        support.shutdown();
        TestUtil.assertThrows( 
                null, 
                IllegalStateException.class, new BlastContainer()
                {
                    public void test()
                        {
                            support.verifyNotShutdown();
                        }
                    } );
    }
    
    
    private final class ConcreteShutdownable extends BaseShutdownable
    {
        // empty
    }
}
