/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.reflect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ReflectUtil_Test 
{
    @Test
    public void testConvertEnumDoesSo()
    {
        assertEquals(HpComputers.GOOD, ReflectUtil.convertEnum( DellComputers.class, HpComputers.class, DellComputers.GOOD ), "Shoulda converted enum.");
        assertEquals(HpComputers.BAD, ReflectUtil.convertEnum( DellComputers.class, HpComputers.class, DellComputers.BA_D ), "Shoulda converted enum.");
        assertEquals(HpComputers.VERY_GOOD, ReflectUtil.convertEnum( DecComputers.class, HpComputers.class, DecComputers.VERY_GOOD ), "Shoulda converted enum.");
    }
    
    
    @Test
    public void testConvertEnumWhenTypeCannotBeConvertedDueToMissingEnumNotAllowed()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                ReflectUtil.convertEnum( HpComputers.class, DellComputers.class, HpComputers.GOOD );
            }
        } );
    }
    
    
    @Test
    public void testConvertEnumWhenTypeCannotBeConvertedDueToMultipleEnumsNotAllowed()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                ReflectUtil.convertEnum( HpComputers.class, DecComputers.class, HpComputers.GOOD );
            }
        } );
    }
    
    
    private enum HpComputers
    {
        VERY_GOOD,
        GOOD,
        BAD
    }
    
    
    private enum DellComputers
    {
        GOOD,
        BA_D
    }
    
    
    private enum DecComputers
    {
        VERYGOOD,
        VERY_GOOD,
        GOOD,
        BAD
    }
    
    
    @Test
    public void testToObjectArrayReturnsExpectedValue()
    {
        assertNull( ReflectUtil.toObjectArray( null ),
                "Null input shoulda resulted in null output." );
        assertArrayEquals(new Integer[] {}, ReflectUtil.toObjectArray( new int[] {} ), "Empty int[] shoulda resulted in an empty Integer[].");
        assertArrayEquals(new Integer[] { Integer.valueOf( 256 ), Integer.valueOf( 1 ) }, ReflectUtil.toObjectArray( new int[] { 256, 1 } ), "int[] with items shoulda resulted in an Integer[] with items.");
        assertArrayEquals(new Long[] {}, ReflectUtil.toObjectArray( new long[] {} ), "Empty long[] shoulda resulted in an empty Long[].");
        assertArrayEquals(new Long[] { Long.valueOf( 256 ), Long.valueOf( 1 ) }, ReflectUtil.toObjectArray( new long[] { 256, 1 } ), "long[] with items shoulda resulted in an Long[] with items.");
        assertArrayEquals(new Double[] {}, ReflectUtil.toObjectArray( new double[] {} ), "Empty double[] shoulda resulted in an empty Double[].");
        assertArrayEquals(new Double[] { Double.valueOf( 256.1 ), Double.valueOf( 1.1 ) }, ReflectUtil.toObjectArray( new double[] { 256.1, 1.1 } ), "double[] with items shoulda resulted in an Double[] with items.");
        assertArrayEquals(new Boolean[] {}, ReflectUtil.toObjectArray( new boolean[] {} ), "Empty boolean[] shoulda resulted in an empty Boolean[].");
        assertArrayEquals(new Boolean[] { Boolean.valueOf( true ), Boolean.valueOf( false ) }, ReflectUtil.toObjectArray( new boolean[] { true, false } ), "boolean[] with items shoulda resulted in an Boolean[] with items.");
    }
    
    

    @Test
    public void testToObjectArrayWithNonArrayThrowsIllegalArgumentException()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                ReflectUtil.toObjectArray( Integer.valueOf( THE_ANSWER ) );
            }
        });
    }
    
    
    @Test
    public void testToObjectArrayWithNonPrimitiveWorks()
    {
        final Integer[] testIntegerArray = new Integer[THE_ANSWER];
        for( int arrayElement = 0; arrayElement < THE_ANSWER; arrayElement++ ) 
        {
            testIntegerArray[0] = Integer.valueOf(arrayElement);
        }
        final Integer[] resultArray = (Integer[])ReflectUtil.toObjectArray( testIntegerArray );
        assertEquals(
                THE_ANSWER,
                resultArray.length,
                "Integer[] with items shoulda resulted in an Integer[] with items."
                 );
        
        for( int arrayTest = 0; arrayTest < THE_ANSWER; arrayTest++ )
        {
            assertEquals(  testIntegerArray[arrayTest], resultArray[arrayTest] );
        }
    }
    
    
    @Test
    public void testToNonPrimitiveTypeNullTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                ReflectUtil.toNonPrimitiveType( null );
            }
        } );
    }
    
    
    @Test
    public void testToNonPrimitiveTypeDoesSo()
    {
        assertEquals(String.class, ReflectUtil.toNonPrimitiveType( String.class ), "Shoulda kept non-primitive type.");
        assertEquals(Integer.class, ReflectUtil.toNonPrimitiveType( Integer.class ), "Shoulda kept non-primitive type.");
        assertEquals(Integer.class, ReflectUtil.toNonPrimitiveType( int.class ), "Shoulda converted to non-primitive type.");
        assertEquals(Long.class, ReflectUtil.toNonPrimitiveType( long.class ), "Shoulda converted to non-primitive type.");
        assertEquals(Boolean.class, ReflectUtil.toNonPrimitiveType( boolean.class ), "Shoulda converted to non-primitive type.");
    }
    
    
    @Test
    public void testEnumValueOfConvertsCorrectly()
    {
        for( Bikes testBike : Bikes.values() )
        {
            final Bikes testEnum = ReflectUtil.enumValueOf( Bikes.class, testBike.toString() );
            assertEquals( testBike, testEnum );
        }
    }
    
    
    @Test
    public void testEnumValueOfInvalidEnumThrowsRuntimeException()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    ReflectUtil.enumValueOf( Bikes.class, "SONY" );
                }
            } );
    }
    
    
    private enum Bikes 
    {
        YAMAHA,
        SUZUKI,
        KAWASAKI,
        BMW,
        HARLEY
    }
    
    
    private static final int THE_ANSWER = 42;
}
