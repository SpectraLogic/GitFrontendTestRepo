/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;

public interface RpcCompletedListener< T >
{
    /**
     * @param future of the request that has completed, either successfully or due to an error
     */
    void remoteProcedureRequestCompleted( final RpcFuture< T > future );
}
