/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class DatabaseSupportException_Test 
{
    @Test
    public void testWarnDoesNotBlowUp()
    {
        final DatabaseSupportException databaseSupportException =
                new DatabaseSupportException( "exception message" );
        databaseSupportException.warn( "Test warning message." );
    }
}
