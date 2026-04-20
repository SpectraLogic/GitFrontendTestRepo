/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

public final class RpcRequestUnserviceableException extends RuntimeException
{
    RpcRequestUnserviceableException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
