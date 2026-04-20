/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.lang.ByteRanges;
import com.spectralogic.util.lang.math.LongRange;
import com.spectralogic.util.lang.math.LongRangeImpl;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ByteRangesImpl_Test 
{

    @Test
    public void testToByteRangesParsesStringInputCorrectly()
    {
        ByteRanges ranges = new ByteRangesImpl( "bytes=0-0", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(0,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(0,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(0,  ranges.getFullRequiredRange().getStart(), "Shoulda reported range correctly.");
        assertEquals(0,  ranges.getFullRequiredRange().getEnd(), "Shoulda reported range correctly.");
        assertEquals(1,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=0-100", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(0,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(0,  ranges.getFullRequiredRange().getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getFullRequiredRange().getEnd(), "Shoulda reported range correctly.");
        assertEquals(101,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=10-100", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(10,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(91,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( " b Y t E s  =10-100", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(10,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(91,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=-1", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(9999,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(9999,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(1,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=-100", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(9900,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(9999,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=9900-", 10000 );
        assertEquals(1,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(9900,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(9999,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=0-100,9900-", 10000 );
        assertEquals(2,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(0,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(9900,  ranges.getRanges().get(1).getStart(), "Shoulda reported range correctly.");
        assertEquals(9999,  ranges.getRanges().get(1).getEnd(), "Shoulda reported range correctly.");
        assertEquals(201,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=0 - 100 , 9900 -", 10000 );
        assertEquals(2,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(0,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(9900,  ranges.getRanges().get(1).getStart(), "Shoulda reported range correctly.");
        assertEquals(9999,  ranges.getRanges().get(1).getEnd(), "Shoulda reported range correctly.");
        assertEquals(201,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");

        ranges = new ByteRangesImpl( "bytes=0-100, 300-400, 500-600", 10000 );
        assertEquals(3,  ranges.getRanges().size(), "Shoulda reported correct number of ranges.getRanges().");
        assertEquals(0,  ranges.getRanges().get(0).getStart(), "Shoulda reported range correctly.");
        assertEquals(100,  ranges.getRanges().get(0).getEnd(), "Shoulda reported range correctly.");
        assertEquals(300,  ranges.getRanges().get(1).getStart(), "Shoulda reported range correctly.");
        assertEquals(400,  ranges.getRanges().get(1).getEnd(), "Shoulda reported range correctly.");
        assertEquals(500,  ranges.getRanges().get(2).getStart(), "Shoulda reported range correctly.");
        assertEquals(600,  ranges.getRanges().get(2).getEnd(), "Shoulda reported range correctly.");
        assertEquals(0,  ranges.getFullRequiredRange().getStart(), "Shoulda reported range correctly.");
        assertEquals(600,  ranges.getFullRequiredRange().getEnd(), "Shoulda reported range correctly.");
        assertEquals(303,  ranges.getAggregateLength(), "Shoulda reported correct aggregate length.");
    }
    
    
    @Test
    public void testToByteRangesToStringReturnsSensibleStringFormat()
    {
        assertEquals("bytes 0-100,300-400,500-600/10000", new ByteRangesImpl( "bytes=0-100, 300-400, 500-600", 10000 ).toString(), "Shoulda formatted byte ranges sensibly.");
    }
    
    
    @Test
    public void testToByteRangesWhenByteRangesOverlapNotAllowed()
    {
        TestUtil.assertThrows( "Byte 100 overlaps", GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=0-100, 500-600, 300-400, 100-200", 10000 );
            }
        } );
    }
    
    
    @Test
    public void testToByteRangesWhenNoRangesNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "", 10000 );
            }
        } );
    }
    
    
    @Test
    public void testToByteRangesWhenInvalidRangesNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "0-10", 10000 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=-", 10000 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=300-400,100-200", 10000 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=43857", 10000 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=4944-5000-6000", 10000 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=0-0,3388", 10000 );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new ByteRangesImpl( "bytes=0-10000", 10000 );
            }
        } );
    }
    
    
    @Test
    public void testShiftReturnsSameInstanceWhenOffsetIsZero()
    {
        final ByteRanges byteRanges = new ByteRangesImpl( "bytes=100-149,200-249", 1000L );
        assertSame(
                byteRanges,
                byteRanges.shift( 0 ),
                "Should notta changed by shifting by zero." );
    }


    @Test
    public void testShiftReturnsShiftedOffsets()
    {
        final ByteRanges byteRanges = new ByteRangesImpl( "bytes=100-149,200-249", 1000L );
        final ByteRanges result = byteRanges.shift( -12L );

        assertEquals(byteRanges.getAggregateLength(), result.getAggregateLength(), "Shoulda had the same aggregate length.");

        final List< LongRange > ranges = result.getRanges();
        assertEquals(2,  ranges.size(), "Shoulda returned the correct number of ranges.");
        assertEquals(new LongRangeImpl( 88, 137 ), ranges.get( 0 ), "Shoulda shifted the first range.");
        assertEquals(new LongRangeImpl( 188, 237 ), ranges.get( 1 ), "Shoulda shifted the second range.");
    }
}
