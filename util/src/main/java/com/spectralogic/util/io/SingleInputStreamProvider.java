/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.InputStream;

import com.spectralogic.util.io.lang.InputStreamProvider;
import com.spectralogic.util.lang.Validations;

public final class SingleInputStreamProvider implements InputStreamProvider
{
    public SingleInputStreamProvider( final InputStream is )
    {
        Validations.verifyNotNull( "Input stream", is );
        m_is = is;
    }


    synchronized public InputStream getNextInputStream()
    {
        final InputStream retval = m_is;
        m_is = null;
        return retval;
    }
    
    
    private InputStream m_is;
}
