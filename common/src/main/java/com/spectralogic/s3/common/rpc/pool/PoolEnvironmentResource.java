/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.rpc.pool.domain.PoolEnvironmentInformation;
import com.spectralogic.s3.common.rpc.pool.domain.PoolInformation;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

@RpcResourceName( "PoolEnvironment" )
public interface PoolEnvironmentResource extends RpcResource
{
    /**
     * Any pool that we cannot talk to or that is currently initializing or otherwise unavailable is not to 
     * be included in the response payload.  Note that powered off pools are not exempt from the response
     * payload, or in other words, if a pool would be reported if it were powered on, it should also be
     * reported if it is powered off.
     */
    @RpcMethodReturnType( PoolEnvironmentInformation.class )
    RpcFuture< PoolEnvironmentInformation > getPoolEnvironment();
    
    
    /**
     * Gets the specified pool by {@link PoolObservable#getGuid}
     */
    @RpcMethodReturnType( PoolInformation.class )
    RpcFuture< PoolInformation > getPool( final String poolGuid );
    
    
    /**
     * Performs low-level verification (including error detection and correction if possible) of the specified
     * pool.  This call could take hours or even days to complete.  <br><br>
     * 
     * Clients invoking this method are responsible for powering on the pool and leaving it powered on during
     * verification.  Note that if a client dies while a pool is being verified, the client may call
     * {@link #quiesceState} upon restart and may not issue a new invocation to verify the pool immediately.
     * Implementations of this method are required to handle this case gracefully.  They may either (i) abort
     * the verification to power down the pool, (ii) temporarily halt the verification to power down the pool
     * but restart it once the pool is powered on again, or (iii) leave the pool powered on and continue the 
     * verification.  <br><br>
     * 
     * If this method is called on a pool that is already being verified (whether due to a previous 
     * {@link #verifyPool} invocation or due to a verify initiated in some other way not through this RPC 
     * API), the method invocation should block waiting for the in-progress verification to complete, rather 
     * than starting a whole new one.  <br><br>
     * 
     * There may be multiple {@link #verifyPool} invocations that correlate to the same, single pool verify.
     * In this case, only the most recent {@link #verifyPool}invocation for a given pool must be responded to 
     * once that pool verification completes.  For example, if a {@link #verifyPool} invocation is made (A) 
     * which initiates a verify, then another {@link #verifyPool} invocation is made on the same pool (B) 
     * while the verify  from (A) is still in-progress, then once the verify originally initiated by (A) 
     * completes, (B) is the only {@link #verifyPool} invocation that must be responded to that the verify 
     * completed successfully.   While implementations are allowed to send a notification for more 
     * invocations (e.g. both (A) and (B)), this is not a requirement and clients of this method should not 
     * assume that any invocation other than the most recent will be responded to.  Also, note that if 
     * multiple {@link #verifyPool} invocations are made concurrently but for different pools, every one of 
     * those invocations should be processed concurrently and responded to upon completion.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > verifyPool( final String poolGuid );
    
    
    /**
     * Destroys all data on the pool specified.  Does so without individually deleting files one at a time,
     * but rather, by using a format or similar request, to improve performance.  This method should return
     * in less than a minute regardless as to how many files were on the pool being formatted.  <br><br>
     * 
     * Upon completion, the pool still exists, but it is empty (contains no data).
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > formatPool( final String poolGuid );
    
    
    /**
     * Destroys all data on the pool specified.  Upon completion, the pool no longer exists.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > destroyPool( final String poolGuid );
    
    
    /**
     * The pool environment generation number will be incremented whenever the pool environment changes.  
     * <br><br>
     * 
     * Used and available capacity changes on pools do not qualify as a change worthy of bumping the pool
     * environment generation number.  Clients that perform operations that may change a pool's capacity may
     * call {@link #getPool} upon completion to update capacity information.  <br><br>
     * 
     * See {@link #powerOn} and {@link #powerOff} for their contracts with regards to pool environment
     * generation number changes.
     */
    @RpcMethodReturnType( Long.class )
    RpcFuture< Long > getPoolEnvironmentGenerationNumber();
    
    
    /**
     * Quiesces the pool environment into a clean state where there are no pending operations.  Calling this
     * method effectively calls {@link #powerOff} for every pool.  <br><br>
     * 
     * This method will block and not return until the pool environment is in this state.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > quiesceState();
    
    
    /**
     * Powers on the specified pool.  Blocks and does not return until the pool is up and ready to have I/O
     * driven to/from it.  <br><br>
     * 
     * Note that power up events for pools that occur as a result of this call do not require a generation
     * number bump for the pool environment i.e. since the client has performed an operation with a
     * deterministic result for power change, the client can update its pool environment state upon successful
     * completion of this invocation.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > powerOn( final String poolGuid );
    
    
    /**
     * Marks that the pool may be powered down, but implementations are not required to immediately power
     * down.  For example, an implementation may hold off powering down a pool up to a timeout in case a
     * {@link #powerOn} command is sent soon after this method is called.  <br><br>
     * 
     * Does not block (this method should immediately return, even before the pool is actually powered off).
     * <br><br>
     * 
     * Once a pool is actually powered down (which shall occur asynchronously to any invocation of this 
     * method), a generation number bump for the pool environment is required i.e. since a client request to 
     * power off a pool does not have a deterministic result for power change (implementors may choose to 
     * leave the pool powered on for some additional time in case it needs to be powered up in the very near 
     * future to avoid too many power management operations on the pool), the client must rely on a pool 
     * environment generation number change to determine that a pool has actually been powered down.  This 
     * information may be important to a client, who may try to schedule work for powered-on pools over 
     * powered-off pools to avoid excessive power management operations on pools.  <br><br>
     * 
     * Clients may not rely on the current power state of a pool as reported in the pool environment for the
     * purpose of assuming a pool is powered on and can be written to.  For example, if a client invokes this
     * method, but the pool is still being reported as powered on, it is illegal for the client to perform
     * any operations on the pool that require it to be powered on.  The client must first invoke
     * {@link #powerOn}, to ensure the pool is still powered on and will continue to be powered on for the
     * entire duration of the operation the client needs to perform that requires the pool to be powered on.
     * Failure to comply may result in the pool being powered off during said client operation.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > powerOff( final String poolGuid );
    
    
    /**
     * Writes the specified pool id to the pool, thereby taking ownership of the pool.  See 
     * {@link PoolInformation#getPoolId} for more information about what the pool's pool id means.  <br><br>
     * 
     * Clients are required to have the pool powered on to make this call.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > takeOwnershipOfPool( final String poolGuid, final UUID poolId );
}
