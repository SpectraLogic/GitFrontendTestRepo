/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.s3target;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class S3SdkFailure_Test
{
    @Test
    public void testValueOfAlwaysReturnsSameInstance()
    {
        final S3SdkFailure failure1a = S3SdkFailure.valueOf( "a", 111 );
        final S3SdkFailure failure1b = S3SdkFailure.valueOf( "a", 111 );
        final S3SdkFailure failure2a = S3SdkFailure.valueOf( "a", 622 );
        final S3SdkFailure failure1c = S3SdkFailure.valueOf( "a", 111 );
        final S3SdkFailure failure2b = S3SdkFailure.valueOf( "a", 622 );
        final S3SdkFailure failure3 = S3SdkFailure.valueOf( "b", 622 );
        assertTrue(failure1a == failure1b, "Same code shoulda returned same instance.");
        assertTrue(failure1c == failure1b, "Same code shoulda returned same instance.");
        assertTrue(failure2a == failure2b, "Same code shoulda returned same instance.");
        assertFalse(failure1a == failure2b, "Same code shoulda returned same instance.");
        assertFalse(failure3 == failure2b, "Same code shoulda returned same instance.");
    }

    @Test
    public void testToStringReturnsNonNull()
    {
        assertNotNull(S3SdkFailure.valueOf( "a", 2 ).toString(), "Shoulda returned non-null.");
    }
    
    @Test
    public void testGetHttpResponseCodeReturnsCorrectly()
    {
        assertEquals(2, S3SdkFailure.valueOf("a", 2).getHttpResponseCode(), "Shoulda returned correct code.");
    }
    
    @Test
    public void testGetCodeReturnsCorrectly()
    {
        assertNotNull(S3SdkFailure.valueOf( "a", 2 ).getCode(), "Shoulda returned sensible code.");
        assertFalse(S3SdkFailure.valueOf( "a", 2 ).getCode().contains( "2" ), "Shoulda returned sensible code.");
        assertTrue(S3SdkFailure.valueOf( "a", 2 ).getCode().contains( "a" ), "Shoulda returned sensible code.");
    }
}
