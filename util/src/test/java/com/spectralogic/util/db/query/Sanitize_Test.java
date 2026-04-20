/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class Sanitize_Test 
{
    @Test
    public void testPatternLiteralEscapesSpecialChars()
    {
        checkPatternLiteral( "foo", "foo" );
        checkPatternLiteral( "\\_%foo", "\\\\\\_\\%foo" );
        checkPatternLiteral( "\\f_o%o", "\\\\f\\_o\\%o" );
    }
    
    
    @Test
    public void testPatternLiteralUsesProvidedStringBuilder()
    {
        assertEquals(
                "foo%\\%bar\\_baz",
                Sanitize.patternLiteral( new StringBuilder( "foo%" ), "%bar_baz" ).toString(),
                "Shoulda escaped just the pattern literal parts."
                 );
    }
    
    
    private static void checkPatternLiteral( final String input, final String expectedOutput )
    {
        assertEquals(
                expectedOutput,
                Sanitize.patternLiteral( input ).toString(),
                "Shoulda escaped the pattern literal."
                 );
    }
}
