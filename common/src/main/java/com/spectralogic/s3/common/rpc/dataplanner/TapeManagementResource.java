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
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.QuiescableRpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

/**
 * The {@link RpcResource} for the data planner to manage tapes.
 */
@RpcResourceName( "TapeManager" )
public interface TapeManagementResource extends QuiescableRpcResource
{
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > ejectStorageDomain(
            final UUID storageDomainId,
            @NullAllowed final UUID bucketId,
            @NullAllowed final String ejectLabel, 
            @NullAllowed final String ejectLocation,
            @NullAllowed final UUID [] blobIds );
    

    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > ejectTape( 
            @NullAllowed final UUID tapeId,
            @NullAllowed final String ejectLabel, 
            @NullAllowed final String ejectLocation );
    

    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > onlineTape( @NullAllowed final UUID tapeId );
    

    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > cancelEjectTape( @NullAllowed final UUID tapeId );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > cancelFormatTape( @NullAllowed final UUID tapeId );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > cancelImportTape( @NullAllowed final UUID tapeId );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > cancelOnlineTape( @NullAllowed final UUID tapeId );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > cancelVerifyTape( @NullAllowed final UUID tapeId );


    @RpcMethodReturnType( void.class )
    @NullAllowed RpcFuture< ? > cancelTestDrive(final UUID driveId );
    
    
    /**
     * @param force        - If true, will format the tape even if it has data on it
     * @param characterize - whether to characterize the tape upon format
     */
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > formatTape(
            @NullAllowed final UUID tapeId, final boolean force, boolean characterize);
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > importTape( 
            @NullAllowed final UUID tapeId, 
            final ImportPersistenceTargetDirectiveRequest importDirective );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > rawImportTape( 
            @NullAllowed final UUID tapeId, 
            final UUID bucketId,
            final ImportPersistenceTargetDirectiveRequest importDirective );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > inspectTape(
            @NullAllowed final UUID tapeId,
            @NullAllowed final BlobStoreTaskPriority priority );
    
    
    @RpcMethodReturnType( TapeFailuresInformation.class )
    @NullAllowed RpcFuture< TapeFailuresInformation > verifyTape(
            @NullAllowed final UUID tapeId,
            @NullAllowed final BlobStoreTaskPriority priority );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cleanDrive( final UUID driveId );


    @RpcMethodReturnType( void.class )
    RpcFuture< ? > testDrive(final UUID driveId, @NullAllowed final UUID tapeId, boolean cleanFirst);


    @RpcMethodReturnType( void.class )
    RpcFuture< ? > driveDump(UUID driveId);

    /**
     * Deletes all tape drives associated with the partition specified, the tape partition itself, and any
     * tapes in the partition specified that do not contain data on them.  Updates any tapes in the partition
     * specified that have data on them to not being in any partition, also transisting their state to 
     * {@link TapeState#EJECTED}.
     * 
     * @throws RpcProxyException if the tape partition to delete is not offline or does not exist
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteOfflineTapePartition( final UUID partitionId );
    

    /**
     * Deletes the specified tape drive, de-associating the tape that was last in the drive with the drive,
     * if applicable.
     * 
     * @throws RpcProxyException if the tape drive to delete is not offline or does not exist
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deleteOfflineTapeDrive( final UUID driveId );
    
    
    /**
     * Forces the tape environment to be refreshed at the earliest convenience.  Note that the tape 
     * environment is updated automatically based on tape environment change events.  Invoking this method
     * should never be necessary unless an event is missed and thus we fail to update the tape environment.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > forceTapeEnvironmentRefresh();


    /**
     * Flags the tape environment to be refreshed next time its generation is checked. Differs from the
     * "forceTapeEnvironmentRefresh()" call in that it is purely a flag and makes no attempt to synchronously
     * update the environment.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > flagEnvironmentForRefresh();
    

    /**
     * Deletes the specified tape as being permanently lost (can only be invoked for a tape that is in state
     * {@link TapeState#LOST}.  <br><br>
     * 
     * Any data that resided on that tape will be forgotten and {@link DegradedBlob} records created as 
     * necessary to record the data loss that has occurred.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > deletePermanentlyLostTape( final UUID tapeId );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > refreshStorageDomainAutoEjectCronTriggers();
}
