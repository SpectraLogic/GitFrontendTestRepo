/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import org.apache.log4j.Logger;

/**
 * Database support is not always available.  Tests that require it must fail when database support is
 * unavailable, since if we skip or pass those tests, it gives whoever is running the tests the false
 * impression that things are fine when a potentially substantial subset of the tests couldn't be run.
 */
public final class DatabaseSupportException extends RuntimeException
{
    public DatabaseSupportException( final String message )
    {
        super( message );
    }
    
    
    public void warn( final String message )
    {
        LOG.warn( "Database support is unavailable.  " + message );
    }
    
    
    private final static Logger LOG = Logger.getLogger( DatabaseSupportException.class );
}
