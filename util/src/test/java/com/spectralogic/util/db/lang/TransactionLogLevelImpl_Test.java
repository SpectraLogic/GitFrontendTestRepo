/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;

public final class TransactionLogLevelImpl_Test 
{
    @Test
    public void testDefaultLevelIsDebug()
    {
        final TransactionLogLevel tll = new TransactionLogLevelImpl();
        assertEquals(
                Level.DEBUG,
                tll.getLevel(),
                "Shoulda returned debug by default."
                );
    }
    
    
    @Test
    public void testLevelTakesOnHighestLevelAdded()
    {
        final TransactionLogLevel tll = new TransactionLogLevelImpl();
        tll.add( Priority.DEBUG_INT );
        tll.add( Priority.INFO_INT );
        tll.add( Priority.DEBUG_INT );
        assertEquals(
                Level.INFO,
                tll.getLevel(),
                "Shoulda returned the highest log level added."
                );
    }
}
