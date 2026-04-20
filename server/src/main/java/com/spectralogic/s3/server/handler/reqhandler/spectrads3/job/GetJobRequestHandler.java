/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.s3.common.dao.domain.ds3.CompletedJob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.JobStatus;
import com.spectralogic.s3.server.domain.JobWithChunksApiBean;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetJobRequestHandler extends BaseRequestHandler
{
    public GetJobRequestHandler()
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.STANDARD,
                BucketAclPermission.JOB, 
                AdministratorOverride.YES ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.SHOW,
                RestDomainType.JOB ) );
    }
    

    @Override
    synchronized protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final UUID jobId = request.getRestRequest().getId(
                params.getServiceManager().getRetriever( Job.class ) );
        params.getPlannerResource().cleanUpCompletedJobsAndJobChunks().get( Timeout.DEFAULT );
        if ( null != params.getServiceManager().getRetriever( CompletedJob.class ).retrieve( jobId ) )
        {
            return BeanServlet.serviceGet(
                    params,
                    getCompletedJobRetval( jobId, params, JobStatus.COMPLETED ) );
        }
        if ( null != params.getServiceManager().getRetriever( CanceledJob.class ).retrieve( jobId ) )
        {
            return BeanServlet.serviceGet(
                    params,
                    getCompletedJobRetval( jobId, params, JobStatus.CANCELED ) );
        }
        
        return BeanServlet.serviceGet( 
                params,
                new JobResponseBuilder( 
                        jobId,
                        params ).buildFromDatabase() );
    }
    
    
    private JobWithChunksApiBean getCompletedJobRetval( 
            final UUID jobId,
            final CommandExecutionParams params,
            final JobStatus status )
    {
        final JobResponseBuilder jrb = new JobResponseBuilder( 
                                                  jobId,
                                                  params );
        final JobWithChunksApiBean retval;
        if ( JobStatus.COMPLETED == status )
        {
            retval = jrb.< CompletedJob >initializeResponse( CompletedJob.class );
        }
        else
        {
            retval = jrb.< CanceledJob >initializeResponse( CanceledJob.class );
        }
        retval.setStatus( status );
        return retval;
    }
}
