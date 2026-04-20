/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservableException;

public abstract class RpcException extends FailureTypeObservableException
{
    protected RpcException( final FailureType failureCode, final String message )
    {
        super( failureCode, message );
    }
    
    
    protected RpcException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
    
    
    protected RpcException( final Throwable cause )
    {
        super( cause );
    }
}
