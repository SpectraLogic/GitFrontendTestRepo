/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape;

import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

/**
 * A tape library is made up of partitions.  Over SCSI, when we want to talk with a library, we really talk to
 * a partition.  No matter how many libraries and partitions there are, we will have a single media changer
 * RPC resource instance per Bluestorm appliance.  Thus, the instance name for this resource type should
 * always be null or an empty string.
 */
@RpcResourceName( "TapeEnvironment" )
public interface TapeEnvironmentResource extends RpcResource
{
    /**
     * The tape partitions, element address blocks, tapes, and tape drives that we can talk to are to be be
     * interrogated and asynchronously updated in the background, so that the tape environment information is
     * always readily available and this request can be serviced immediately.  <br><br>
     * 
     * Any library/partition that we cannot talk to or that is currently initializing or otherwise unavailable
     * is not to be included in the response payload. <br><br>
     * 
     * If a move command is in progress, this servicing of this request will not block.  The tape environment
     * returned will be consistent and describe either the state before the move or the state afterward, and
     * not an inconsistent state in between.
     */
    @RpcMethodReturnType( TapeEnvironmentInformation.class )
    RpcFuture< TapeEnvironmentInformation > getTapeEnvironment();
    
    
    /**
     * This command always operates in less than a second.  <br><br>
     * 
     * The tape environment generation number will be incremented whenever the tape environment changes,
     * unless the change was a result of a move command, in which case the caller knows about the change
     * already since the caller initiated the change.
     */
    @RpcMethodReturnType( Long.class )
    RpcFuture< Long > getTapeEnvironmentGenerationNumber();
    
    
    /**
     * Quiesces the tape environment into a clean state where there are no pending operations (like moves,
     * reads or writes).  This method will block and not return until the tape environment is in this stat
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > quiesceState();
}
