/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import com.spectralogic.util.exception.GenericFailure;

public final class RpcTimeoutException extends RpcException
{
    RpcTimeoutException( final Exception ex )
    {
        super( ex );
    }
    
    
    RpcTimeoutException( final String message )
    {
        super( GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, message );
    }
}
