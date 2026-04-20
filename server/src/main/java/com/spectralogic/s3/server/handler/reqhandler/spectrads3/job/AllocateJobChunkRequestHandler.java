/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.client.RpcException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class AllocateJobChunkRequestHandler extends BaseRequestHandler
{
    public AllocateJobChunkRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.ALLOCATE,
                       RestDomainType.JOB_CHUNK ) );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        params.getPlannerResource().cleanUpCompletedJobsAndJobChunks().get( Timeout.DEFAULT );
        final JobEntry chunk = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( JobEntry.class ) );
        final Job job = params.getServiceManager().getRetriever( Job.class ).attain( chunk.getJobId() );
        params.getPlannerResource().jobStillActive( job.getId(), null );
        if ( JobRequestType.PUT != job.getRequestType() )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "Chunks cannot be explicitly allocated for " + job.getRequestType() + " jobs." );
        }
        if ( job.isAggregating() )
        {
            LOG.warn( "Chunks cannot be explicitly allocated for aggregating jobs, since aggregating jobs " 
                      + "are always entirely pre-allocated and their chunking structure is volatile.  "
                      + "Will not throw exception per BLKP-2728." );
        }
        
        try
        {
            params.getPlannerResource().allocateEntry( chunk.getId() ).get( Timeout.LONG );
        }
        catch ( final RpcException ex )
        {
            /*
             * If we can't allocate the cache space immediately, return that fact immediately rather than
             * blocking in the S3 server.
             */
            if ( GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT.getHttpResponseCode() 
                    == ex.getFailureType().getHttpResponseCode() )
            {
                throw new S3RestException(
                        GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT,
                        "Failed to allocate cache space.",
                        ex ).setRetryAfter( CACHE_ALLOCATION_RETRY_AFTER_IN_SECONDS );
            }
            throw ex;
        }

        return new GetJobChunkRequestHandler().handleRequestInternal( request, params );
    }
    
    
    private static final int CACHE_ALLOCATION_RETRY_AFTER_IN_SECONDS = 5 * 60;
}
