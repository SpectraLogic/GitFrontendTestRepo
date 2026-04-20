/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.HashSet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class Validations_Test 
{
    @Test
    public void testVerifyNotNullNullParamNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                Validations.verifyNotNull( null, "bob" );
            }
        } );
    }
    
    
    @Test
    public void testVerifyNotNullNullParamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                Validations.verifyNotNull( "Name", null );
            }
        } );
    }
    
    
    @Test
    public void testVerifyNotNullReturnsIfNoProblems()
    {
        Validations.verifyNotNull( "Name", "bob" );
    }


    @Test
    public void testVerifyNotEmptyStringNullParamNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyNotEmptyString( null, "not empty string" );
                }
            } );
    }


    @Test
    public void testVerifyNotEmptyStringEmptyStringParamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyNotEmptyString( "Name", "" );
                }
            } );
    }
    
    
    @Test
    public void testVerifyNotEmptyStringReturnsIfNoProblems()
    {
        Validations.verifyNotEmptyString( "Name", "not empty string" );
    }


    @Test
    public void testVerifyNotEmptyCollectionNullCollectionParamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyNotEmptyCollection( "Name", null );
                }
            } );
    }


    @Test
    public void testVerifyNotEmptyCollectionEmptyCollectionParamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyNotEmptyCollection( "Name", new HashSet<>() );
                }
            } );
    }
    
    
    @Test
    public void testVerifyNotEmptyCollectionReturnsIfNoProblems()
    {
        Validations.verifyNotEmptyCollection( "Name", CollectionFactory.toSet( this ) );
    }
    

    @Test
    public void testVerifyInRangeIntNullParamNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( null, 0, 2, 1 );
                }
            } );
    }
    

    @Test
    public void testVerifyInRangeLongNullParamNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( null, 0L, 2L, 1L );
                }
            } );
    }
    
    
    @Test
    public void testVerifyInRangeLongNullParamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                    {
                        Validations.verifyInRange( "Count", 0, 2, (Integer)null );
                    }
                } );
    }
    
    
    @Test
    public void testVerifyInRangeIntNullParamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( "Count", 0L, 2L, (Long)null );
                }
            } );
    }
    
    
    @Test
    public void testVerifyInRangeIntWhenNotInRangeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( "Count", 0, 2, -1 );
                }
            } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( "Count", 0, 2, 3 );
                }
            } );
    }
    
    
    @Test
    public void testVerifyInRangeLongWhenNotInRangeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( "Count", 0L, 2L, -1L );
                }
            } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    Validations.verifyInRange( "Count", 0L, 2L, 3L );
                }
            } );
    }
    
    
    @Test
    public void testVerifyInRangeIntReturnsIfNoProblems()
    {
        Validations.verifyInRange( "Count", 0, 2, 0 );
        Validations.verifyInRange( "Count", 0, 2, 1 );
        Validations.verifyInRange( "Count", 0, 2, 2 );
    }
    
    
    @Test
    public void testVerifyInRangeLongReturnsIfNoProblems()
    {
        Validations.verifyInRange( "Count", 0L, 2L, 0L );
        Validations.verifyInRange( "Count", 0L, 2L, 1L );
        Validations.verifyInRange( "Count", 0L, 2L, 2L );
    }
}
