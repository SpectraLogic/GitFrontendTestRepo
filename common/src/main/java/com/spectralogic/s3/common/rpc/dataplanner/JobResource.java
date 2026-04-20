/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;

public interface JobResource
{
    /**
     * @return UUID of the job started, or null if no job needed to be created for the request (e.g. if the
     * request only creates folders, then no job shall be created, and thus, no job id returned)
     */
    @RpcMethodReturnType( UUID.class )
    RpcFuture< UUID > createPutJob( final CreatePutJobParams params );
    
    
    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cancelJob( @NullAllowed final UUID userId, final UUID jobId, final boolean force );


    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cancelJobQuietly( @NullAllowed final UUID userId, final UUID jobId, final boolean force );


    @RpcMethodReturnType( void.class )
    RpcFuture< ? > cleanUpCompletedJobsAndJobChunks();


    @RpcMethodReturnType( void.class )
    RpcFuture< ? > modifyJob(final UUID jobId, final BlobStoreTaskPriority priority);
}
