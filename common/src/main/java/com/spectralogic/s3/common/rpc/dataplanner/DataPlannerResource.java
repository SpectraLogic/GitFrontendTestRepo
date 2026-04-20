/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.rpc.dataplanner.domain.*;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.QuiescableRpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;
import com.spectralogic.util.security.ChecksumType;

/**
 * The main {@link RpcResource} for the data planner, containing methods to create jobs, perform I/O, etc.
 */
@RpcResourceName( "DataPlanner" )
public interface DataPlannerResource extends QuiescableRpcResource, DeleteObjectsResource, JobResource
{
    @NullAllowed
    @RpcMethodReturnType( UUID.class )
    /**
     * @param job - Contains all necessary details about the Put job we are replicating.
     * 
     * @return UUID of the resultant Put job
     */
    RpcFuture< UUID > replicatePutJob( final DetailedJobToReplicate job );
    
    
    /**
     * Creates both a restore-style GET job and PUT job to move data from one place to another.
     * (e.g. Staging and IOM jobs). These two jobs serve as each other's "clients" so no further
     * client action is required after jobs are created this way.
     *  
     * @param params - Parameters that describe the jobs to be created.
     * @param persistenceTargetInfo - Contains info about which storage domains and targets need to be persisted to
     * @return UUID of the resultant Put job
     */
    @NullAllowed
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createIomJob(
            final CreateGetJobParams params,
            final PersistenceTargetInfo persistenceTargetInfo);
    
    
    /**
     * @param jobId - id of the aggregating job we will be closing
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > closeAggregatingJob( final UUID jobId );
    
    
    /**
     * @param params - if null, then the job is being created locally; else, the job was created
     * elsewhere and is being serviced in part or whole here
     * 
     * @return UUID of the job started
     */
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createGetJob( final CreateGetJobParams params );
    
    
    /**
     * @return UUID of the job started
     */
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createVerifyJob( final CreateVerifyJobParams params );
    
    
    /**
     * Jobs will be marked as dead if nothing happens wrt the job for long enough (for example, no I/O).  You
     * may call this method with a job ID, blob ID, or both a job ID and blob ID.  Note that performance may
     * be adversely impacted if you call this method with a null job ID.
     */
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > jobStillActive( @NullAllowed final UUID jobId, @NullAllowed final UUID blobId );
    
    
    /**
     * @return the object ids that have been deleted as a result of the job cancellation
     */
    Set< UUID > cancelJobInternal( final UUID jobId, final boolean force );


    /**
     * @return the object ids that have been deleted as a result of the job cancellation
     */
    Set< UUID > cancelJobQuietlyInternal( final UUID jobId, final boolean force);


    /**
     * @return a pair containing the file system absolute path for the blob and whether that file is on pool or cache
     * <br><br>
     * If the blob is not in cache, will throw an exception.
     */
    @RpcMethodReturnType( DiskFileInfo.class )
    RpcFuture<DiskFileInfo> startBlobRead(@NullAllowed final UUID jobId, final UUID blobId );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > blobReadCompleted( @NullAllowed final UUID jobId, final UUID blobId );
    

    /**
     * @return the file system absolute path that the blob should be written into
     * <br><br>
     * If cache space cannot be allocated for the blob, will throw an exception.
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > startBlobWrite( @NullAllowed final UUID jobId, final UUID blobId );
    

    /**
     * @param objectMetadata - metadata for object. If this method is being invoked for the first time for a
     * given object, a null or empty value indicates no metadata and a non-null, non-empty value specifies
     * the object metadata. Object metadata is always defined on the first invocation of this method for a
     * given object, and any subsequent calls for the same object cannot modify the object metadata defined
     * on the first invocation. For subsequent invocations, if null or empty is specified, nothing will
     * happen; however, if a non-null, non-empty value is specified, the object metadata sent down on the
     * subsequent invocation will be validated to ensure it matches the already-defined object metadata from
     * the first invocation, and a failure will be thrown if there is not an identical match. 
     *
     * @return null if the object has not completed; or
     *         TRUE if the object completed with a custom creation date and ETag; or
     *         FALSE if the object completed without a custom creation date or ETag
     *         
     * @throws {@link GenericFailure#BAD_REQUEST} if the object tried to complete with a custom creation
     * date or a custom ETag (but not both) i.e. either BOTH a custom creation date and ETag must be defined,
     * or neither can be defined
     */
    @NullAllowed
    @RpcMethodReturnType( Boolean.class )
    RpcFuture< Boolean > blobWriteCompleted( 
            @NullAllowed final UUID jobId,
            final UUID blobId, 
            final ChecksumType checksumType,
            final String checksum,
            @NullAllowed final Long creationDate,
            @NullAllowed final S3ObjectProperty [] objectMetadata );
    

    /**
     * @return file system absolute path for the dumped heap
     */
    @RpcMethodReturnType( String.class )
    RpcFuture< String > dumpHeap();
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > forceFullCacheReclaimNow();
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > forceTargetEnvironmentRefresh();
    
    
    @RpcMethodReturnType( CacheInformation.class )
    RpcFuture< CacheInformation > getCacheState( final boolean includeCacheEntries );
    
    
    /**
     * @return return the blob ids that were both (i) specified in the <code>blobIds</code> param and (ii)
     * in cache 
     */
    @RpcMethodReturnType( BlobsInCacheInformation.class )
    RpcFuture< BlobsInCacheInformation > getBlobsInCache( final UUID [] blobIds );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > allocateEntry(final UUID entryId );


    @RpcMethodReturnType( EntriesInCacheInformation.class )
    RpcFuture<EntriesInCacheInformation> allocateEntries(final boolean allowPartial, final UUID[] entryIds );
    
    
    @RpcMethodReturnType( Boolean.class )
    RpcFuture< Boolean > isChunkEntirelyInCache( final UUID chunkId );
    
    
    /**
     * @return all the tasks across all the data stores that have one of the states in set of states sent
     */
    @RpcMethodReturnType( BlobStoreTasksInformation.class )
    RpcFuture< BlobStoreTasksInformation > getBlobStoreTasks( final BlobStoreTaskState [] states );


    /**
     * @return all the tasks across all the data stores that have one of the states in set of states sent for given job
     */
    @RpcMethodReturnType( BlobStoreTasksInformation.class )
    RpcFuture< BlobStoreTasksInformation > getBlobStoreTasksForJob(@NullAllowed final UUID jobId, final BlobStoreTaskState [] states);
    
    
    @RpcMethodReturnType( LogicalUsedCapacityInformation.class )
    RpcFuture< LogicalUsedCapacityInformation > getLogicalUsedCapacity( final UUID [] bucketIds );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cleanUpCompletedJobsAndJobChunks();
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > validateFeatureKeysNow();
}
