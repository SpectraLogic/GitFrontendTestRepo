/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.comparator;

import java.util.Comparator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class NullHandlingComparator_Test 
{
    @Test
    public void testComparatorHandlesNullsCorrectlyWhenDecoratedHandler()
    {
        final NullHandlingComparator comparator = 
                new NullHandlingComparator( new MockComparator() );
        assertTrue(0 == comparator.compare( null, null ), "Shoulda returned correct comparison result.");
        assertTrue(0 > comparator.compare( null, "a" ), "Shoulda returned correct comparison result.");
        assertTrue(0 < comparator.compare( "a", null ), "Shoulda returned correct comparison result.");

        final String a = "a";
        final String b = "b";
        assertEquals(a.hashCode() - b.hashCode(),  comparator.compare(a, b), "Shoulda returned correct comparison result.");

        final Object o1 = new Object();
        final Object o2 = new Object();
        final int compareResult = comparator.compare( o1, o2 );
        if ( o1.hashCode() > o2.hashCode() )
        {
            assertTrue(0 < compareResult, "Shoulda returned correct comparison result.");
        }
        else
        {
            assertTrue(0 > compareResult, "Shoulda returned correct comparison result.");
        }
    }
    
    
    @Test
    public void testComparatorHandlesNullsCorrectlyWhenNullDecoratedHandler()
    {
        final NullHandlingComparator comparator = 
                new NullHandlingComparator( null );
        assertTrue(0 == comparator.compare( null, null ), "Shoulda returned correct comparison result.");
        assertTrue(0 > comparator.compare( null, "a" ), "Shoulda returned correct comparison result.");
        assertTrue(0 < comparator.compare( "a", null ), "Shoulda returned correct comparison result.");

        assertTrue(0 < comparator.compare( "b", "a" ), "Shoulda returned correct comparison result.");
        assertTrue(0 > comparator.compare( "a", "b" ), "Shoulda returned correct comparison result.");

        TestUtil.assertThrows(
                null, 
                UnsupportedOperationException.class, new BlastContainer()
                {
                    public void test()
                        {
                            comparator.compare( new Object(), new Object() );
                        }
                    } );
    }
    
    
    private final static class MockComparator implements Comparator< Object >
    {
        public int compare( final Object o1, final Object o2 )
        {
            return o1.hashCode() - o2.hashCode();
        }
    } // end inner class def
}
