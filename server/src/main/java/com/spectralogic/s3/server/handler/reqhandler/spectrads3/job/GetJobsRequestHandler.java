/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.s3.common.dao.domain.ds3.CompletedJob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.JobApiBean;
import com.spectralogic.s3.server.domain.JobStatus;
import com.spectralogic.s3.server.domain.JobWithChunksApiBean;
import com.spectralogic.s3.server.domain.JobsApiBean;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetJobsRequestHandler extends BaseDaoTypedRequestHandler< Job >
{
    public GetJobsRequestHandler()
    {
        super( Job.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.LIST,
                       RestDomainType.JOB ) );
        
        registerOptionalBeanProperties( JobObservable.BUCKET_ID );
        registerOptionalRequestParameters( RequestParameterType.FULL_DETAILS );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final Job clientSpecifiedJob = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.YES );
        final Bucket bucket = ( null == clientSpecifiedJob.getBucketId() ) ?
                null
                : params.getServiceManager().getRetriever( Bucket.class ).attain(
                        clientSpecifiedJob.getBucketId() );

        params.getPlannerResource().cleanUpCompletedJobsAndJobChunks().get( Timeout.DEFAULT );
        
        final List< JobApiBean > retval = new ArrayList<>();
        Set< Job > jobs = params.getServiceManager().getRetriever( Job.class ).retrieveAll( Require.all( 
                Require.any( 
                        Require.beanPropertyEquals( 
                                UserIdObservable.USER_ID, 
                                request.getAuthorization().getUserId() ),
                        Require.beanPropertyEqualsOneOf( 
                                JobObservable.BUCKET_ID, 
                                BucketAuthorization.getBucketsUserHasAccessTo(
                                        SystemBucketAccess.STANDARD, 
                                        BucketAclPermission.JOB, 
                                        AdministratorOverride.YES,
                                        params ) ) ),
                ( null == bucket ) ?
                        Require.nothing() 
                        : Require.beanPropertyEquals( JobObservable.BUCKET_ID, bucket.getId() ) ) ).toSet();
        jobs = BeanUtils.sort( jobs );
        for ( final Job job : jobs )
        {
            retval.add( constructJobResponse( params, job, JobStatus.IN_PROGRESS ) );
        }
        
        if ( request.hasRequestParameter( RequestParameterType.FULL_DETAILS ) )
        {
            final Set< CompletedJob > completedJobs = BeanUtils.sort( 
                    params.getServiceManager().getRetriever( CompletedJob.class ).retrieveAll().toSet() );
            for ( final CompletedJob job : completedJobs )
            {
                retval.add( constructJobResponse( params, job, JobStatus.COMPLETED ) );
            }
            
            final Set< CanceledJob > canceledJobs = BeanUtils.sort( 
                    params.getServiceManager().getRetriever( CanceledJob.class ).retrieveAll().toSet() );
            for ( final CanceledJob job : canceledJobs )
            {
                retval.add( constructJobResponse( params, job, JobStatus.CANCELED ) );
            }
        }

        final JobsApiBean retvalContainer = BeanFactory.newBean( JobsApiBean.class );
        retvalContainer.setJobs( CollectionFactory.toArray( JobApiBean.class, retval ) );
        return BeanServlet.serviceGet( params, retvalContainer );
    }
    
    
    private < T extends JobObservable< ? > & Identifiable > JobApiBean constructJobResponse(
            final CommandExecutionParams params,
            final T job,
            final JobStatus status )
    {
        final JobWithChunksApiBean jobChunksApiBean = new JobResponseBuilder( 
                job.getId(),
                params ).initializeResponse( job );
        final JobApiBean jobApiBean = BeanFactory.newBean( JobApiBean.class );
        BeanCopier.copy( jobApiBean, jobChunksApiBean );
        jobApiBean.setStatus( status );
        return jobApiBean;
    }
}
