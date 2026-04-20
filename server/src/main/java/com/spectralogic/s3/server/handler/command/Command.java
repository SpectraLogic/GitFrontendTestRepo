/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.command;

import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;

public interface Command< R >
{
    public R execute( final CommandExecutionParams commandExecutionParams );
}
