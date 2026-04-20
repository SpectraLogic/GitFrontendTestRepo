/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.log;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.log4j.Logger;

public final class LoggingUncaughtExceptionHandler implements UncaughtExceptionHandler
{
    private LoggingUncaughtExceptionHandler()
    {
        // singleton
    }
    
    
    public static LoggingUncaughtExceptionHandler getInstance()
    {
        return INSTANCE;
    }
    
    
    public void uncaughtException( final Thread t, final Throwable e )
    {
        LOG.error( "Unhandled exception occurred.", e );
    }

    
    private final static Logger LOG = Logger.getLogger( LoggingUncaughtExceptionHandler.class );
    private final static LoggingUncaughtExceptionHandler INSTANCE = new LoggingUncaughtExceptionHandler();
}
