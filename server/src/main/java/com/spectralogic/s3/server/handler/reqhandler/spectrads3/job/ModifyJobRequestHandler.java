/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Date;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public class ModifyJobRequestHandler extends BaseModifyBeanRequestHandler< Job >
{
    public ModifyJobRequestHandler()
    {
        this( RestDomainType.JOB );
    }
    
    
    protected ModifyJobRequestHandler( final RestDomainType domain )
    {
        super( Job.class,
               new BucketAuthorizationStrategy( 
                    SystemBucketAccess.INTERNAL_ONLY,
                    BucketAclPermission.JOB, 
                    AdministratorOverride.YES ),
               domain );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                JobObservable.CREATED_AT, 
                JobObservable.PRIORITY,
                Job.PROTECTED,
                Job.DEAD_JOB_CLEANUP_ALLOWED);
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            DS3Request request,
            CommandExecutionParams params )
    {
        super.handleRequestInternal( request, params );
        return BeanServlet.serviceModify( 
                params,
                new JobResponseBuilder( 
                        request.getRestRequest().getId(
                                params.getServiceManager().getRetriever( Job.class ) ),
                        params ).buildFromDatabase() );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final Job bean,
            final Set< String > modifiedProperties )
    {
        super.modifyBean( params, bean, modifiedProperties );
        if ( modifiedProperties.contains( JobObservable.PRIORITY ) )
        {
            params.getTargetResource().modifyJob(bean.getId(), bean.getPriority()).get( Timeout.LONG );
        }
    }

    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final Job bean, 
            final Set< String > modifiedProperties )
    {
        final Date now = new Date();
        if ( modifiedProperties.contains( JobObservable.CREATED_AT ) 
                && bean.getCreatedAt().getTime() > now.getTime() )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "You can move the creation date of a job into the past, but not into the future.  "
                    + bean.getCreatedAt() + " is after " + now + "." );
        }
        
        // We wait until this completes to ensure that the job doesn't get "cleaned up" due to inactivity
        params.getPlannerResource().jobStillActive( bean.getId(), null ).get( Timeout.LONG );
    }
}
