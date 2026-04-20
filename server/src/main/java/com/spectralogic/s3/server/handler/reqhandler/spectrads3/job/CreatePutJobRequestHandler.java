/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CreatePutJobParams;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.CreateJob;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public final class CreatePutJobRequestHandler extends BaseDaoTypedRequestHandler< Job >
{
    public CreatePutJobRequestHandler()
    {
        super( Job.class,
               new BucketAuthorizationStrategy( 
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.WRITE,
                AdministratorOverride.NO ), 
               new RestfulCanHandleRequestDeterminer(
                RestOperationType.START_BULK_PUT,
                RestDomainType.BUCKET ) );
        
        registerOptionalRequestParameters(
                RequestParameterType.FORCE,
                RequestParameterType.PRE_ALLOCATE_JOB_SPACE,
                RequestParameterType.MAX_UPLOAD_SIZE,
                RequestParameterType.IGNORE_NAMING_CONFLICTS );
        registerOptionalBeanProperties( 
                JobObservable.PRIORITY,
                NameObservable.NAME,
                Job.AGGREGATING,
                Job.MINIMIZE_SPANNING_ACROSS_MEDIA,
                Job.IMPLICIT_JOB_ID_RESOLUTION,
                Job.VERIFY_AFTER_WRITE,
                Job.PROTECTED,
                Job.DEAD_JOB_CLEANUP_ALLOWED );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Bucket bucket = request.getRestRequest().getBean( 
                params.getServiceManager().getRetriever( Bucket.class ) );
        final DataPolicy dataPolicy = params.getServiceManager().getRetriever( DataPolicy.class ).attain(
                bucket.getDataPolicyId() );
        if ( !request.hasRequestParameter( RequestParameterType.FORCE )
                && !dataPolicy.isAlwaysForcePutJobCreation() )
        {
            VerifySafeToCreatePutJobRequestHandler.verifySafeToCreatePutJob(
                    params.getServiceManager(),
                    bucket.getDataPolicyId() );
        }
        
        request.setBucketName( bucket.getName() );
        final Job job = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.NO );
        job.setRequestType( JobRequestType.PUT )
           .setChunkClientProcessingOrderGuarantee( JobChunkClientProcessingOrderGuarantee.IN_ORDER )
           .setBucketId( bucket.getId() );
        if ( !request.getBeanPropertyValueMapFromRequestParameters().containsKey( Job.VERIFY_AFTER_WRITE ) )
        {
            job.setVerifyAfterWrite( dataPolicy.isDefaultVerifyAfterWrite() );
        }
        
        return new CreateJob( 
                job,
                BlobbingPolicy.ENABLED, 
                null ).execute( params );
    }
}
