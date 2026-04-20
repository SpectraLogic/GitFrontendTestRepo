/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.service.ds3.DataPolicyService;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public class CancelAllJobsRequestHandler extends BaseDaoTypedRequestHandler< Job >
{
    public CancelAllJobsRequestHandler()
    {
        this( RestDomainType.JOB );
    }
    
    
    protected CancelAllJobsRequestHandler( final RestDomainType domain )
    {
        super( Job.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.BULK_DELETE,
                domain ) );
        
        registerOptionalBeanProperties( JobObservable.REQUEST_TYPE, JobObservable.BUCKET_ID );
        registerRequiredRequestParameters( RequestParameterType.FORCE );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final Job sj = getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.NO );
        final WhereClause filter = Require.all(
                Require.beanPropertyEquals( Job.PROTECTED, false ),
                ( null == sj.getRequestType() ) ? 
                        null 
                        : Require.beanPropertyEquals( JobObservable.REQUEST_TYPE, sj.getRequestType() ),
                ( null == sj.getBucketId() ) ? 
                        null 
                        : Require.beanPropertyEquals( JobObservable.BUCKET_ID, sj.getBucketId() ) );
        
        RuntimeException e = null;
        for ( final Job job 
                : params.getServiceManager().getRetriever( Job.class ).retrieveAll( filter ).toSet() )
        {
            try
            {
                final Bucket bucket = 
                        params.getServiceManager().getRetriever( Bucket.class ).attain( job.getBucketId() );
                final JobResource resource = 
                        ( params.getServiceManager().getService( DataPolicyService.class ).isReplicated(
                                bucket.getDataPolicyId() ) ) ? 
                                        params.getTargetResource() 
                                        : params.getPlannerResource();
                //Make sure the job still exists - this reduces the chances of long-running "cancel all" requests
                //conflicting, and also avoids 404's when canceling an IOM job indirectly cancels its counterpart 
                if ( null != params.getServiceManager().getRetriever( Job.class ).retrieve( job.getId() ) )
                {
	                resource.cancelJob( 
	                        request.getAuthorization().getUserId(),
	                        job.getId(),
	                        params.getRequest().hasRequestParameter( RequestParameterType.FORCE ) )
	                            .get( Timeout.LONG );
                }
            }
            catch ( final RuntimeException ex )
            {
                e = ex;
                LOG.warn( "Failed to cancel job " + job.getId() + ".", ex );
            }
        }
        if ( null != e )
        {
            throw e;
        }
        
        return BeanServlet.serviceDelete( params, null );
    }
}
