/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.util.ArrayList;
import java.util.HashSet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;


public final class TestUtil_Test 
{
    @Test
    public void testAssertSameThrowsRuntimeExceptionWithMismatchedLists()
    {
        final ArrayList< Integer > testListA = new ArrayList< >();
        testListA.add(Integer.valueOf( 1 ));
        testListA.add(Integer.valueOf( 2 ));
        
        final ArrayList< Integer > testListB = new ArrayList< >();
        testListB.add(Integer.valueOf( 2 ));
        testListB.add(Integer.valueOf( 1 ));
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.assertSame( testListA, testListB );
            }
        });
    }
    
    
    @Test
    public void testAssertSameThrowsRuntimeExceptionWithNullExpectedList()
    {
        final ArrayList< Integer > testListA = new ArrayList< >();
        testListA.add(Integer.valueOf( 1 ));
        testListA.add(Integer.valueOf( 2 ));
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.assertSame( null, testListA );
            }
        });
    }
    
    
    @Test
    public void testAssertSameThrowsRuntimeExceptionWithNullActualList()
    {
        final ArrayList< Integer > testListA = new ArrayList< >();
        testListA.add(Integer.valueOf( 1 ));
        testListA.add(Integer.valueOf( 2 ));
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.assertSame( testListA, null );
            }
        });
    }
    
    
    @Test
    public void testAssertSamePassesWhenCompareListToItself()
    {
        final ArrayList< Integer > testListA = new ArrayList< >();
        testListA.add(Integer.valueOf( 1 ));
        testListA.add(Integer.valueOf( 2 ));
        
        TestUtil.assertSame( testListA, testListA );
    }
    
    
    @Test
    public void testAssertSamePassesWhenCompareTwoDuplicateLists()
    {
        final ArrayList< Integer > testListA = new ArrayList< >();
        testListA.add(Integer.valueOf( 1 ));
        testListA.add(Integer.valueOf( 2 ));
        
        final ArrayList< Integer > testListB = new ArrayList< >();
        testListB.add(Integer.valueOf( 1 ));
        testListB.add(Integer.valueOf( 2 ));
        
        TestUtil.assertSame( testListA, testListB );
    }
    
    
    @Test
    public void testAssertSameThrowsRuntimeExceptionWithMismatchedSets()
    {
        final HashSet< Integer > testSetA = new HashSet< >();
        testSetA.add(Integer.valueOf( 1 ));
        testSetA.add(Integer.valueOf( 2 ));
        
        final HashSet< Integer > testSetB = new HashSet< >();
        testSetA.add(Integer.valueOf( 3 ));
        testSetA.add(Integer.valueOf( 4 ));
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.assertSame( testSetA, testSetB );
            }
        });
    }
    
    
    @Test
    public void testAssertSameThrowsRuntimeExceptionWithNullExpectedSet()
    {
        final HashSet< Integer > testSetA = new HashSet< >();
        testSetA.add(Integer.valueOf( 1 ));
        testSetA.add(Integer.valueOf( 2 ));
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.assertSame( null, testSetA );
            }
        });
    }
    
    
    @Test
    public void testAssertSameThrowsRuntimeExceptionWithNullActualSet()
    {
        final HashSet< Integer > testSetA = new HashSet< >();
        testSetA.add(Integer.valueOf( 1 ));
        testSetA.add(Integer.valueOf( 2 ));
        
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TestUtil.assertSame( testSetA, null );
            }
        });      
    }
    
    
    @Test
    public void testAssertSamePassesWhenCompareSetToItself()
    {
        final HashSet< Integer > testSetA = new HashSet< >();
        testSetA.add(Integer.valueOf( 1 ));
        testSetA.add(Integer.valueOf( 2 ));
        
        TestUtil.assertSame( testSetA, testSetA );    
    }
    
    
    @Test
    public void testAssertSamePassesWhenCompareTwoDuplicateSets()
    {
        final HashSet< Integer > testSetA = new HashSet< >();
        testSetA.add(Integer.valueOf( 1 ));
        testSetA.add(Integer.valueOf( 2 ));
        
        final HashSet< Integer > testSetB = new HashSet< >();
        testSetB.add(Integer.valueOf( 1 ));
        testSetB.add(Integer.valueOf( 2 ));
        
        TestUtil.assertSame( testSetA, testSetB );
    }
    
    
    @Test
    public void testAssertThrowsThrowsRuntimeExceptionIfWrongExceptionThrown()
    {
        try
        {
            TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    Integer.valueOf( null ); // Should throw NumberFormatException
                }
            });
        }
        catch ( final Throwable ex )
        {
            if( !ex.getClass().isAssignableFrom( RuntimeException.class ) )
            {
                throw new RuntimeException( 
                        "Expected " + RuntimeException.class + ", but " 
                        + ex.getClass().getSimpleName() + " was thrown.", ex );
            }
        }
    }
    
    
    @Test
    public void testAssertThrowsThrowsRuntimeExceptionIfExpectedExceptionNotThrownByTest()
    {
        try
        {
            TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    Integer.valueOf( 42 ); 
                }
            });
            fail( "Expected " + RuntimeException.class + ", but no exception was thrown." );
        }
        catch ( final Throwable ex )
        {
            if( !ex.getClass().isAssignableFrom( RuntimeException.class ) )
            {
                throw new RuntimeException( 
                        "Expected " + RuntimeException.class + ", but " 
                        + ex.getClass().getSimpleName() + " was thrown.", ex );
            }
        }
    }
}
