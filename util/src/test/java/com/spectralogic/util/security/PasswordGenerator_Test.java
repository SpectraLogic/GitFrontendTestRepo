/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class PasswordGenerator_Test 
{
    @Test
    public void testGenerateWithNegativeLengthNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    PasswordGenerator.generate( -1 );
                }
            } );
    }
    
    
    @Test
    public void testGenerateWithZeroLengthReturnsMinimumLengthString()
    {
        assertEquals(
                7,
                PasswordGenerator.generate( 0 ).length(),
                "Shoulda generated password with minimum length."
               );
    }
    
    
    @Test
    public void testGenerateReturnsValidPasswords()
    {
        for ( int i = 0; i < 10; ++i )
        {
            final String password = PasswordGenerator.generate( 10 );
            assertFalse(
                    password.contains( "o" ),
                    "Should notta contained chars that can be confused with other chars."
                    );
            assertFalse(
                    password.contains( "O" ),
                    "Should notta contained chars that can be confused with other chars."
                     );
            assertFalse(
                    password.contains( "0" ),
                    "Should notta contained chars that can be confused with other chars."
                     );
        }
    }
    
    
    @Test
    public void testGenerateWithPositiveLengthReturnsNonEmptyString()
    {
        assertEquals(
                7,
                PasswordGenerator.generate( 1 ).length(),
                "Shoulda generated password with minimum length."
                );
        assertEquals(
                10,
                PasswordGenerator.generate( 10 ).length(),
                "Shoulda generated password with requested length."
                 );
        assertEquals(
                100,
                PasswordGenerator.generate( 100 ).length(),
                "Shoulda generated password with requested length."
                 );
        assertEquals(
                1000,
                PasswordGenerator.generate( 1000 ).length(),
                "Shoulda generated password with requested length."
                 );
    }
    
    
    @Test
    public void testVerifyNullPasswordNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    PasswordGenerator.verify( null );
                }
            } );
    }
    
    
    @Test
    public void testVerifyPasswordTooShortNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                PasswordGenerator.verify( "123456" );
            }
            } );
    }
    
    
    @Test
    public void testVerifyPasswordWithIllegalCharNotAllowed()
    {
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                PasswordGenerator.verify( "12345678\u9883" );
            }
            } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                PasswordGenerator.verify( ";12345678" );
            }
            } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                PasswordGenerator.verify( "1234-5678" );
            }
            } );
    }
    
    
    @Test
    public void testVerifyLegalPasswordAllowed()
    {
        PasswordGenerator.verify( "12345678" );
        PasswordGenerator.verify( "012345678" );
        PasswordGenerator.verify( "1234567" );
        PasswordGenerator.verify( "12345678aBc" );
        PasswordGenerator.verify( "12345678al1" );
    }
}
