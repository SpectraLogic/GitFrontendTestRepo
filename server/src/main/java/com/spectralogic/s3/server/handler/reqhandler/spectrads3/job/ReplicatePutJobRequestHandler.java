/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.ReplicatePutJobRequestHandler.ReplicatePutJobParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ReplicatePutJobRequestHandler extends BaseDaoTypedRequestHandler< ReplicatePutJobParams >
{
    public ReplicatePutJobRequestHandler()
    {
        super( ReplicatePutJobParams.class,
               new BucketAuthorizationStrategy( 
                SystemBucketAccess.STANDARD,
                BucketAclPermission.WRITE,
                AdministratorOverride.NO ), 
               new RestfulCanHandleRequestDeterminer(
                RestOperationType.START_BULK_PUT,
                RestDomainType.BUCKET ) );
        
        registerRequiredRequestParameters( 
                RequestParameterType.REPLICATE );
        registerOptionalBeanProperties( 
                ReplicatePutJobParams.PRIORITY );
    }
    
    
    interface ReplicatePutJobParams extends SimpleBeanSafeToProxy
    {
        String PRIORITY = "priority";
        
        @Optional
        BlobStoreTaskPriority getPriority();
        
        ReplicatePutJobParams setPriority( final BlobStoreTaskPriority value );
    } // end inner class def
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request,
            final CommandExecutionParams params )
    {
        if ( !params.getServiceManager().getRetriever( DataPathBackend.class )
                    .attain( Require.nothing() )
                    .isAllowNewJobRequests() )
        {
            throw new S3RestException( GenericFailure.FORBIDDEN, "New job creation is currently disabled." );
        }
    
        final Bucket bucket = request.getRestRequest().getBean( 
                params.getServiceManager().getRetriever( Bucket.class ) );
        final DataPolicy dataPolicy = params.getServiceManager().getRetriever( DataPolicy.class ).attain(
                bucket.getDataPolicyId() );
        
        final JobToReplicate jobToReplicate;
        try
        {
            jobToReplicate = JsonMarshaler.unmarshal(
                    JobToReplicate.class, 
                    IOUtils.toString( request.getHttpRequest().getInputStream(), Charset.defaultCharset() ) );
        }
        catch ( final Exception ex )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST, 
                    "Cannot parse job to replicate from client.", ex );
        }
        
        final ReplicatePutJobParams paramsSpecified = 
                getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.YES );
        if ( null == paramsSpecified.getPriority() )
        {
            paramsSpecified.setPriority( params.getServiceManager().getRetriever( DataPolicy.class ).attain(
                    bucket.getDataPolicyId() ).getDefaultPutJobPriority() );
        }
        
        final DetailedJobToReplicate req = BeanFactory.newBean( DetailedJobToReplicate.class )
                .setJob( jobToReplicate )
                .setBucketId( bucket.getId() )
                .setUserId( request.getAuthorization().getUser().getId() )
                .setPriority( paramsSpecified.getPriority() )
        		.setVerifyAfterWrite( dataPolicy.isDefaultVerifyAfterWrite() );
        
        final UUID jobId = params.getPlannerResource().replicatePutJob( req ).get( Timeout.LONG );
        if ( null == jobId )
        {
            return BeanServlet.serviceCreate( params, jobId );
        }
        return BeanServlet.serviceGet( 
                params,
                new JobResponseBuilder( 
                        jobToReplicate.getId(),
                        params ).buildFromDatabase() );
    }
}
