/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler;

import com.spectralogic.s3.server.handler.canhandledeterminer.CanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

/**
 * Handles a particular kind of S3 request.
 */
public interface RequestHandler
{
    CanHandleRequestDeterminer getCanHandleRequestDeterminer();
    
    
    ServletResponseStrategy handleRequest( final CommandExecutionParams commandExecutionParams );
}
