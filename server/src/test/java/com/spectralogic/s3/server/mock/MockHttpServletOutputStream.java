/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.io.IOException;

import javax.servlet.ServletOutputStream;

public final class MockHttpServletOutputStream extends ServletOutputStream 
{
    @Override
    public void write(int b) throws IOException 
    {
        m_data.append( (char)b );
    }
    

    public String getString() 
    {
        return m_data.toString();
    }

    
    private final StringBuilder m_data = new StringBuilder();
}
