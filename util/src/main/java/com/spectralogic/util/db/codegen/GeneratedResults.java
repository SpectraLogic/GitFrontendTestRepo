/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.lang.Platform;

public final class GeneratedResults
{
    GeneratedResults( final String fileHeader )
    {
        m_fileHeader = fileHeader;
    }
    
    
    void add( final String fileName, final String generatedCode )
    {
        withoutSpacingAdd(
                fileName, Platform.NEWLINE + Platform.NEWLINE + generatedCode );
    }
    
    
    void withoutSpacingAdd( final String fileName, String generatedCode )
    {
        generatedCode = generatedCode + Platform.NEWLINE;
        if ( m_generatedCode.containsKey( fileName ) )
        {
            m_generatedCode.put( 
                    fileName,
                    m_generatedCode.get( fileName ) + generatedCode );
        }
        else
        {
            m_generatedCode.put( fileName, m_fileHeader + generatedCode );
        }
    }
    
    
    /**
     * @return {@code Map <code file name, generated code for file>}
     */
    public Map< String, String > getCodeFiles()
    {
        return new HashMap<>( m_generatedCode );
    }
    
    
    private final Map< String, String > m_generatedCode = new HashMap<>();
    private final String m_fileHeader;
}
