/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;

public interface AuthenticationStrategy
{
    /**
     * @throws S3RestException if authentication fails
     */
    public void authenticate( final CommandExecutionParams commandExecutionParams );
}
