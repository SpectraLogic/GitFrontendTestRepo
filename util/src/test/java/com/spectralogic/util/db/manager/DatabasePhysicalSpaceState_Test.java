/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class DatabasePhysicalSpaceState_Test 
{
    @Test
    public void testForFreeSpaceRatioReturnsReasonableValuesForInputs()
    {
        assertEquals(
                DatabasePhysicalSpaceState.CRITICAL,
                DatabasePhysicalSpaceState.getFreeToTotalDiskSpaceRatioState( .01 ),
                "Shoulda reported critical for very low free space."
                 );
        assertEquals(
                DatabasePhysicalSpaceState.NORMAL,
                DatabasePhysicalSpaceState.getFreeToTotalDiskSpaceRatioState( .3 ),
                "Shoulda reported normal for lots of free space."
                 );
        
        final Set< DatabasePhysicalSpaceState > foundStates = new HashSet<>();
        for ( double i = 0; i < 1; i += 0.01 )
        {
            foundStates.add( DatabasePhysicalSpaceState.getFreeToTotalDiskSpaceRatioState( i ) );
        }
        assertEquals(
                CollectionFactory.toSet( DatabasePhysicalSpaceState.values() ),
                foundStates,
                "Shoulda been able to get to every state."
                 );
    }
    
    
    @Test
    public void testForInvalidFreeSpaceRatioNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                DatabasePhysicalSpaceState.getFreeToTotalDiskSpaceRatioState( 1.01 );
            }
        } );
    }
}
