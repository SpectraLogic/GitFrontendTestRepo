/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

@RpcResourceName( "PoolManager" )
public interface PoolManagementResource extends RpcResource
{
    /**
     * Reclaims the pool in its entirety if possible, and compacts it if necessary otherwise.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > compactPool( 
            @NullAllowed final UUID poolId,
            @NullAllowed final BlobStoreTaskPriority priority );
    

    /**
     * Formats the pool and takes ownership of it (only applicable to {@link PoolState#FOREIGN} pools)
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > formatPool( @NullAllowed final UUID poolId );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > destroyPool( final UUID poolId );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > importPool(
            @NullAllowed final UUID poolId,
            final ImportPersistenceTargetDirectiveRequest importDirective );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cancelImportPool( @NullAllowed final UUID poolId );
    
    
    /**
     * Verifies the pool specified, or all eligible pools if null specified
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > verifyPool( 
            @NullAllowed final UUID poolId,
            @NullAllowed final BlobStoreTaskPriority priority );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cancelVerifyPool( @NullAllowed final UUID poolId );
    
    
    /**
     * Forces the pool environment to be refreshed at the earliest convenience.  Note that the pool 
     * environment is updated automatically based on pool environment change events.  Invoking this method
     * should never be necessary unless an event is missed and thus we fail to update the pool environment.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > forcePoolEnvironmentRefresh();
    

    /**
     * Deletes the specified pool as being permanently lost (can only be invoked for a pool that is in state
     * {@link PoolState#LOST}.  <br><br>
     * 
     * Any data that resided on that pool will be forgotten and {@link DegradedBlob} records created as 
     * necessary to record the data loss that has occurred.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deletePermanentlyLostPool( final UUID poolId );
}
