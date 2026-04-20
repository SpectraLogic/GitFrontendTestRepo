/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;



import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class ChecksumGenerator_Test 
{
    @Test
    public void testChecksumGeneratesCorrectChecksum()
    {
        assertEquals(
                "5d41402abc4b2a76b9719d911017c592",
                ChecksumGenerator.generateMd5( "hello" ),
                "Shoulda generated correct checksum."
                );
        assertEquals(
                "d41d8cd98f00b204e9800998ecf8427e",
                ChecksumGenerator.generateMd5( "" ),
                "Shoulda generated correct checksum."
                 );
        assertEquals(
                "5d41402abc4b2a76b9719d911017c592",
                ChecksumGenerator.generateMd5( "hello" ),
                "Shoulda generated correct checksum."
                );
    }
}
