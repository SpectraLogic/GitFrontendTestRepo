/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public class CancelJobRequestHandler extends BaseRequestHandler
{
    public CancelJobRequestHandler()
    {
        this( RestDomainType.JOB );
    }
    
    
    protected CancelJobRequestHandler( final RestDomainType domain )
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.JOB, 
                AdministratorOverride.YES ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.DELETE,
                domain ) );
        
        registerRequiredRequestParameters( RequestParameterType.FORCE );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final Job job = 
                request.getRestRequest().getBean( params.getServiceManager().getRetriever( Job.class ) );

        if ( job.isProtected() )
        {
            throw new S3RestException( GenericFailure.CONFLICT,
                    "Cannot cancel this job because it is protected by an application. The " + Job.PROTECTED +
                    " flag must be removed before this job can be deleted." );
        }

        final Bucket bucket = 
                params.getServiceManager().getRetriever( Bucket.class ).attain( job.getBucketId() );
        final JobResource resource = 
                ( params.getServiceManager().getService( DataPolicyService.class ).isReplicated(
                        bucket.getDataPolicyId() ) ) ? 
                                params.getTargetResource() 
                                : params.getPlannerResource();
        resource.cancelJob( 
                request.getAuthorization().getUserId(),
                request.getRestRequest().getId( params.getServiceManager().getRetriever( Job.class ) ),
                params.getRequest().hasRequestParameter( RequestParameterType.FORCE ) ).get( Timeout.LONG );
        return BeanServlet.serviceDelete( params, null );
    }
}
