/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.command;

import org.apache.log4j.Logger;

import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.lang.Duration;

abstract class BaseCommand< R > implements Command< R >
{
    final public R execute( final CommandExecutionParams commandExecutionParams )
    {
        final Duration duration = new Duration();
        
        LOG.info( "Executing " + this.getClass().getSimpleName() + "..." );
        final R retval = executeInternal( commandExecutionParams );
        LOG.info( this.getClass().getSimpleName() + " completed in " + duration + "." );
        
        return retval;
    }
    
    
    protected abstract R executeInternal( final CommandExecutionParams params );
    
    
    protected final static Logger LOG = Logger.getLogger( BaseCommand.class );
}
