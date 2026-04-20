/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.util.regex.Pattern;

import com.spectralogic.util.lang.Platform;

public final class MockHttpRequestUtil
{
    public static void assertResponseAsExpected( final String expectedResponse, final String actualResponse )
    {
        for ( final String expected : MockHttpRequestUtil.compact( expectedResponse ).split(
                Pattern.quote( "{ANY}" ) ) )
        {
            if ( !MockHttpRequestUtil.compact( actualResponse ).contains( expected ) )
            {
                throw new RuntimeException( 
                        "Response shoulda contained " + expected + ", but it didn't: " + actualResponse );
            }
        }
    }
    
    
    public static String compact( final String input )
    {
        return input.replace( " ", "" ).replace( Platform.NEWLINE, "" );
    }
}
