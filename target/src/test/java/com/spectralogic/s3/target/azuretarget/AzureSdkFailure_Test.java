/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.azuretarget;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class AzureSdkFailure_Test
{

    @Test
    public void testValueOfAlwaysReturnsSameInstance()
    {
        final AzureSdkFailure failure1a = AzureSdkFailure.valueOf( "a", 111 );
        final AzureSdkFailure failure1b = AzureSdkFailure.valueOf( "a", 111 );
        final AzureSdkFailure failure2a = AzureSdkFailure.valueOf( "a", 622 );
        final AzureSdkFailure failure1c = AzureSdkFailure.valueOf( "a", 111 );
        final AzureSdkFailure failure2b = AzureSdkFailure.valueOf( "a", 622 );
        final AzureSdkFailure failure3 = AzureSdkFailure.valueOf( "b", 622 );

        assertTrue(failure1a == failure1b, "Same code shoulda returned same instance.");
        assertTrue(failure1c == failure1b, "Same code shoulda returned same instance.");
        assertTrue(failure2a == failure2b, "Same code shoulda returned same instance.");
        assertFalse(failure1a == failure2b, "Same code shoulda returned same instance.");
        assertFalse(failure3 == failure2b, "Same code shoulda returned same instance.");
    }
    
    @Test
    public void testToStringReturnsNonNull()
    {
        assertNotNull(AzureSdkFailure.valueOf( "a", 2 ).toString(), "Shoulda returned non-null.");
    }
    
    @Test
    public void testGetHttpResponseCodeReturnsCorrectly()
    {
        assertEquals(2, AzureSdkFailure.valueOf( "a", 2 ).getHttpResponseCode(), "Shoulda returned correct code.");
    }
    
    @Test
    public void testGetCodeReturnsCorrectly()
    {
        assertNotNull(AzureSdkFailure.valueOf( "a", 2 ).getCode(), "Shoulda returned sensible code.");
        assertFalse(AzureSdkFailure.valueOf( "a", 2 ).getCode().contains( "2" ), "Shoulda returned sensible code.");
        assertTrue(AzureSdkFailure.valueOf( "a", 2 ).getCode().contains( "a" ), "Shoulda returned sensible code.");
    }
}
