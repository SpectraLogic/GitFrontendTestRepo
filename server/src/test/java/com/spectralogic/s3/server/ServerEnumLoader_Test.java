/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server;



import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Test;

public final class ServerEnumLoader_Test
{
    @Test
    public void testCanLoadEnumTypes()
    {
        TestUtil.loadStaticallyGeneratedEnumCode( getClass() );
    }
}
