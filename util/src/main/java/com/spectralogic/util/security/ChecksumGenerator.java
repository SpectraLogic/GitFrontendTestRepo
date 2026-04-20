/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class ChecksumGenerator
{
    private ChecksumGenerator()
    {
        // singleton
    }
    
    
    public static String generateMd5( final String contentsToChecksum )
    {
        try 
        {
            final MessageDigest messageDigest = MessageDigest.getInstance( "MD5" );
            final byte[] checksum = messageDigest.digest( contentsToChecksum.getBytes() );
            final StringBuilder retval = new StringBuilder();
            for ( int i = 0; i < checksum.length; ++i ) 
            {
                retval.append( Integer.toHexString( (checksum[i] & 0xFF) | 0x100 ).substring( 1, 3 ) );
            }
            return retval.toString();
        } 
        catch ( final NoSuchAlgorithmException e) 
        {
            throw new RuntimeException( "Failed to generate checksum.", e );
        }
    }
}
