/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
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
import com.spectralogic.util.lang.NamingConventionType;

public final class GetJobToReplicateRequestHandler extends BaseRequestHandler
{
    public GetJobToReplicateRequestHandler()
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.STANDARD,
                BucketAclPermission.JOB, 
                AdministratorOverride.YES ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.SHOW,
                RestDomainType.JOB ) );
        registerRequiredRequestParameters( RequestParameterType.REPLICATE );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Job job = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( Job.class ) );
        final String retval = new JobReplicationSupport( 
                        params.getServiceManager(),
                        job.getId() )
                .getJobToReplicate().getJob().toJson( 
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );
        
        return BeanServlet.serviceGet( 
                params, 
                retval );
    }
}
