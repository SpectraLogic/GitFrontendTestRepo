/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;


/**
 * An {@link RpcResource} that may be quiesced to prepare it for shutdown.
 */
public interface QuiescableRpcResource extends RpcResource
{
    /**
     * Will attempt to quiesce the RPC resource.  If it is not safely quiescable:
     * <ul>
     * <li>And <code>force</code> is <code>false</code>, then an error will be returned
     * <li>And <code>force</code> is <code>true</code>, then the system will be forced to quiesce, which could
     * result in some adverse effect such as the canceling of pending requests.  If the system cannot be 
     * forcibly quiesced (for example, if we are in the middle of an operation that must be quiesced), an 
     * error will be returned.
     * </ul>
     * 
     * Calling this method will leave the RPC resource in a quiesced state, where it will not accept any new
     * work that would require quiescing.  There is no way to get back out of a quiesced state without 
     * restarting the RPC resource.  <br><br>
     * 
     * Calling this method multiple times (whether or not previous invocations returned an error or not) is
     * acceptable.  For example, if you ask for a quiesce and it fails, you may come back later and ask again
     * (even though we will eventually quiesce, asking later is a way to discover if quiescing has completed).
     * You may also ask for a quiesce after a success, in which case quiesce will return immediately with 
     * success.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > quiesceAndPrepareForShutdown( final boolean force );
}
