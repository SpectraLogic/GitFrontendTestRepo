/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 */
public class LtfsNameUtils_Test 
{
    @Test
    public void testGetValidationErrorMessageReturnsNullWhenNameIsShortAscii()
    {
        assertNull(
                LtfsNameUtils.getLtfsValidationErrorMessage( "foo/bar" ),
                "Should notta returned an error message."
                 );
    }
    
    
    @Test
    public void testGetValidationErrorMessageReturnsNullWhenNameContainsColon()
    {
        assertNull(
                LtfsNameUtils.getLtfsValidationErrorMessage( "foo/b:ar" ),
                "Should notta returned an error message."
                );
    }
    
    
    @Test
    public void testGetValidationErrorMessageReturnsNullWhenNameIs1024BytesEncoded()
    {
        assertNull(
                LtfsNameUtils.getLtfsValidationErrorMessage(
                        StringUtils.repeat( "123456789/", 102 )
                                + "12\u0628" ),
                "Should notta returned an error message."
                );
    }
    
    
    @Test
    public void testGetValidationErrorMessageReturnsLengthErrorMessageWhenNameIs1025BytesEncoded()
    {
        assertEquals(
                "Object name was longer than S3 allows.",
                LtfsNameUtils.getLtfsValidationErrorMessage(
                        StringUtils.repeat( "123456789/", 102 )
                                + "123\u0628" ),
                "Shoulda returned an object name length error message."
                 );
    }
    
    
    @Test
    public void testGetValidationErrorMessageReturnsNullWhenComponentIs255Characters()
    {
        assertNull(
                LtfsNameUtils.getLtfsValidationErrorMessage(
                        StringUtils.repeat( "1234567890", 25 )
                                + "1234\u0628/foo" ),
                "Object name was longer that S3 specifications allow."
                 );
    }
    
    
    @Test
    public void testGetValidationErrorMessageReturnsComponentLengthErrorMessageWhenComponentIs256Characters()
    {
        assertEquals(
                "Slash (/) delimited path component was larger than DS3 allows.",
                LtfsNameUtils.getLtfsValidationErrorMessage(
                        StringUtils.repeat( "1234567890", 25 )
                                + "12345\u0628/foo" ),
                "Shoulda returned an object name component length error message."
                 );
    }
    
    @Test
    public void testGetValidationErrorMessageReturnsErrorMessageWhenNameHasConsecutiveSlashes()
    {
        assertEquals(
                "Multiple consecutive slash chars (//) not allowed in LTFS mode.",
                LtfsNameUtils.getLtfsValidationErrorMessage("foo///bar" ),
                "Shoulda returned a consecutive slash character error message."
                 );
    }
    
}
