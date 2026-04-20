/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class Ds3SdkFailure_Test
{
    @Test
    public void testValueOfAlwaysReturnsSameInstance()
    {
        final Ds3SdkFailure failure1a = Ds3SdkFailure.valueOf( 111 );
        final Ds3SdkFailure failure1b = Ds3SdkFailure.valueOf( 111 );
        final Ds3SdkFailure failure2a = Ds3SdkFailure.valueOf( 622 );
        final Ds3SdkFailure failure1c = Ds3SdkFailure.valueOf( 111 );
        final Ds3SdkFailure failure2b = Ds3SdkFailure.valueOf( 622 );
        assertTrue(failure1a == failure1b, "Same code shoulda returned same instance.");
        assertTrue(failure1c == failure1b, "Same code shoulda returned same instance.");
        assertTrue(failure2a == failure2b, "Same code shoulda returned same instance.");
        assertFalse(failure1a == failure2b, "Same code shoulda returned same instance.");
    }

    @Test
    public void testToStringReturnsNonNull()
    {
        assertNotNull(Ds3SdkFailure.valueOf( 2 ).toString(), "Shoulda returned non-null.");
    }

    @Test
    public void testGetHttpResponseCodeReturnsCorrectly()
    {
        assertEquals(2, Ds3SdkFailure.valueOf( 2 ).getHttpResponseCode(), "Shoulda returned correct code.");
    }
    
    @Test
    public void testGetCodeReturnsCorrectly()
    {
        assertNotNull(Ds3SdkFailure.valueOf( 2 ).getCode(), "Shoulda returned sensible code.");
        assertTrue(Ds3SdkFailure.valueOf( 2 ).getCode().contains( "2" ), "Shoulda returned sensible code.");
    }
}
