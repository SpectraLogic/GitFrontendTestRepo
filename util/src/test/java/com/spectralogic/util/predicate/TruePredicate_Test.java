/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.predicate;



import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TruePredicate_Test 
{
    @Test
    public void testTestReturnsTrue()
    {
        final TruePredicate< Object > pred = new TruePredicate< >();
        final boolean result = pred.test( new Object() );
        assertEquals(
                true,
                result,
                "True predicate returns true."  );
    }
}
