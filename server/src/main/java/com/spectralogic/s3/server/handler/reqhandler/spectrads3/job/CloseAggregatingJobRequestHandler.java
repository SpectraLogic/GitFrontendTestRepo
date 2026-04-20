package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public class CloseAggregatingJobRequestHandler extends BaseRequestHandler
{
    public CloseAggregatingJobRequestHandler()
    {
        super(
                new BucketAuthorizationStrategy(
                        SystemBucketAccess.INTERNAL_ONLY,
                        BucketAclPermission.JOB, 
                        AdministratorOverride.YES ),
                new RestfulCanHandleRequestDeterminer(
                        RestActionType.MODIFY, 
                        RestDomainType.JOB ) );
        registerRequiredRequestParameters( RequestParameterType.CLOSE_AGGREGATING_JOB );
    }

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Job job = 
                request.getRestRequest().getBean( params.getServiceManager().getRetriever( Job.class ) );
        params.getPlannerResource().closeAggregatingJob( job.getId() );
        return BeanServlet.serviceModify( 
                params,
                new JobResponseBuilder( 
                        request.getRestRequest().getId(
                                params.getServiceManager().getRetriever( Job.class ) ),
                        params ).buildFromDatabase() );
    }
}
