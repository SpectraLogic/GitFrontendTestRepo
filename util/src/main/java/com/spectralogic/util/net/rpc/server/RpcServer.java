/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.shutdown.Shutdownable;

/**
 * Services incoming remote procedure calls / JiveResource requests.
 */
public interface RpcServer extends Shutdownable
{
    void register( final String instanceName, final RpcResource rpcResource );
    
    
    void unregister( final String instanceName, final Class< ? extends RpcResource > rpcResourceApi );
}
