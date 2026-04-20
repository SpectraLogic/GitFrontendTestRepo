/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;

public final class UtilEnumLoader_Test 
{
    @Test
    public void testCanLoadEnumTypes()
    {
        TestUtil.loadStaticallyGeneratedEnumCode( getClass() );
    }
}
