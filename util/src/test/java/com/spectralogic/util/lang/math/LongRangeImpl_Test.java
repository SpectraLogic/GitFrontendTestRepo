/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.math;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class LongRangeImpl_Test 
{
    @Test
    public void testConstructorEndBeforeStartNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new LongRangeImpl( 22, 21 );
                }
            } );
        new LongRangeImpl( 22, 22 );
        new LongRangeImpl( 22, 23 );
    }
    
    
    @Test
    public void testToStringReturnsNonNullDescriptiveText()
    {
        final LongRange range = new LongRangeImpl( 111, 222 );
        assertTrue(
                range.toString().contains( LongRangeImpl.class.getSimpleName() ),
                "toString shoulda included class simple name.");
        assertTrue(
                range.toString().contains( "111" ),
                "toString shoulda included range endpoints.");
        assertTrue(
                range.toString().contains( "222" ),
                "toString shoulda included range endpoints.");
    }
    
    
    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEqualAndHashAndLength()
    {
        final LongRange rangeZero = new LongRangeImpl( 0, 0 );
        assertTrue(rangeZero.getLength() == 1, "length of 1:");
        
        final LongRange rangeMin = new LongRangeImpl( Long.MIN_VALUE, Long.MIN_VALUE );
        assertTrue(rangeMin.getLength() == 1, "length of 1:");
        
        final LongRange rangeMax = new LongRangeImpl( Long.MAX_VALUE, Long.MAX_VALUE );
        assertTrue(rangeMax.getLength() == 1, "length of 1:");
        
        final LongRange rangeMinMax = new LongRangeImpl( Long.MIN_VALUE, Long.MAX_VALUE );
        final LongRange rangeMinMax2 = new LongRangeImpl( Long.MIN_VALUE, Long.MAX_VALUE );
        
        assertTrue(rangeMinMax.equals( rangeMinMax ), "equals of same object");
        assertTrue(!rangeMinMax.equals( Long.valueOf( 123 ) ), "not equals of other type of object");
        assertTrue(rangeMinMax.equals( rangeMinMax2 ), "equals of objects with same value");
        assertTrue(rangeMinMax.hashCode() == rangeMinMax.hashCode(), "length of 1:");
        
        final LongRange rangeSameStart1 = new LongRangeImpl( 1, 2 );
        final LongRange rangeSameStart2 = new LongRangeImpl( 1, 3 );
        assertTrue(!rangeSameStart1.equals( rangeSameStart2 ), "equals of objects with same start");
        
        final LongRange rangeSameEnd1 = new LongRangeImpl( 1, 3 );
        final LongRange rangeSameEnd2 = new LongRangeImpl( 2, 3 );
        assertTrue(!rangeSameEnd1.equals( rangeSameEnd2 ), "equals of objects with same start");
        
        final LongRange rangeDifferent1 = new LongRangeImpl( 1, 2 );
        final LongRange rangeDifferent2 = new LongRangeImpl( 3, 4 );
        assertTrue(!rangeDifferent1.equals( rangeDifferent2 ), "equals of objects with same start");
    }
    
    
    @Test
    public void testOverlaps()
    {
        final LongRange rangeInside = new LongRangeImpl( -1, 4 );
        final LongRange rangeOutside = new LongRangeImpl( 1, 3 );
        assertTrue(rangeInside.overlaps( rangeOutside ), "overlaps inside:");
        assertTrue(rangeOutside.overlaps( rangeInside ), "overlaps rangeOutside:");
        
        final LongRange rangeOverlapsFirst = new LongRangeImpl( -1, 4 );
        final LongRange rangeOverlapsSecond = new LongRangeImpl( 1, 5 );
        assertTrue(rangeOverlapsFirst.overlaps( rangeOverlapsSecond ), "overlaps rangeOverlapsFirst:");
        assertTrue(rangeOverlapsSecond.overlaps( rangeOverlapsFirst ), "overlaps rangeOverlapsSecond:");
        
        final LongRange rangeOverlapsByOneFirst = new LongRangeImpl( -1, 4 );
        final LongRange rangeOverlapsByOneSecond = new LongRangeImpl( 4, 5 );
        assertTrue(rangeOverlapsByOneFirst.overlaps( rangeOverlapsByOneSecond ),
                "overlaps rangeOverlapsByOneFirst:");
        assertTrue(rangeOverlapsByOneSecond.overlaps( rangeOverlapsByOneFirst ),
                "overlaps rangeOverlapsByOneSecond:");
        
        final LongRange rangeDoesNotOverlapFirst = new LongRangeImpl( -5, -4 );
        final LongRange rangeDoesNotOverlapSecond = new LongRangeImpl( -3, -2 );
        assertTrue(!rangeDoesNotOverlapFirst.overlaps( rangeDoesNotOverlapSecond ),
                "overlaps rangeDoesNotOverlapFirst:");
        assertTrue(!rangeDoesNotOverlapSecond.overlaps( rangeDoesNotOverlapFirst ),
                "overlaps rangeDoesNotOverlapSecond:");
    }    
}
