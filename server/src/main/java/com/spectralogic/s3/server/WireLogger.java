/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server;

import org.apache.log4j.Logger;

public final class WireLogger
{
    private WireLogger()
    {
        // do not instantiate
    }
    
    
    public final static Logger LOG = Logger.getLogger( WireLogger.class );
}
