/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

public final class Sanitize
{
    private Sanitize()
    {
        //empty
    }
    
    
    public static StringBuilder patternLiteral( final String literal )
    {
        return patternLiteral( new StringBuilder(), literal );
    }

    
    public static StringBuilder patternLiteral( final StringBuilder stringBuilder, final String literal )
    {
        for ( int i = 0; i < literal.length(); ++i )
        {
            final char currentChar = literal.charAt( i );
            switch ( currentChar )
            {
                case '%':
                    stringBuilder.append( "\\%" );
                    break;
                case '_':
                    stringBuilder.append( "\\_" );
                    break;
                case '\\':
                    stringBuilder.append( "\\\\" );
                    break;
                default:
                    stringBuilder.append( currentChar );
                    break;
            }
        }
        return stringBuilder;
    }
}
