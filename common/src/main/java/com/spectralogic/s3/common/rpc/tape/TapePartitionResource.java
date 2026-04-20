/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.tape;

import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

/**
 * A tape library is made up of partitions.  Over SCSI, when we want to talk with a library, we really talk to
 * a partition.  The instance name for a tape partition resource is the tape partition's serial number.
 */
@RpcResourceName( "TapePartition" )
public interface TapePartitionResource extends RpcResource
{
    /**
     * Moves the media in the source element address to the destination element address.  For example, to
     * load a tape in a storage element address to a tape drive, specify the tape drive's element address
     * as the destination and the tape's storage element address as the source. <br><br>
     * 
     * This command typically operates on the order of 30 seconds, but can take significantly longer.  
     * <br><br>
     * 
     * Warning: Move commands to a partition must be serialized i.e. it is illegal to execute multiple 
     * move commands concurrently for the same partition.  It is legal to execute multiple move commands 
     * concurrently provided that each move command is for a different partition (e.g. going to a different
     * resource instance).
     * 
     * @throws RpcProxyException with a tape environment changed failure code per 
     * {@link TapeResourceFailureCode} - If this occurs, the client is responsible for catching this
     * exception, re-discovering the tape environment, and then re-issuing the move command if necessary.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > move( 
            final int srcElementAddress, 
            final int destElementAddress );
}
