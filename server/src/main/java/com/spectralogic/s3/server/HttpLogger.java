/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server;

import org.apache.log4j.Logger;

public final class HttpLogger
{
    private HttpLogger()
    {
        // do not instantiate
    }
    
    
    public final static Logger LOG = Logger.getLogger( HttpLogger.class );
}
