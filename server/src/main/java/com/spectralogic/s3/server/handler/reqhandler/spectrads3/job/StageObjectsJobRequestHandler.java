/*******************************************************************************
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/

package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.IomType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.CreateJob;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;

public final class StageObjectsJobRequestHandler extends BaseDaoTypedRequestHandler< Job >
{
    public StageObjectsJobRequestHandler()
    {
        super( Job.class,
                new BucketAuthorizationStrategy(
                        BucketAuthorization.SystemBucketAccess.STANDARD,
                        BucketAclPermission.READ,
                        BucketAclAuthorizationService.AdministratorOverride.NO ),
                new RestfulCanHandleRequestDeterminer(
                        RestOperationType.START_BULK_STAGE,
                        RestDomainType.BUCKET ) );

        registerOptionalBeanProperties(
                JobObservable.PRIORITY,
                NameObservable.NAME );
    }


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
                        
        request.setBucketName( bucket.getName() );
        final Job job = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.NO );
        job.setBucketId( bucket.getId() );
        //We use the GET job type because the get job must be created first
        job.setRequestType( JobRequestType.GET );
        job.setIomType( IomType.STAGE);
        job.setChunkClientProcessingOrderGuarantee( JobChunkClientProcessingOrderGuarantee.NONE );
        job.setName( "Stage" );
        return new CreateJob(
                job,
                BlobbingPolicy.ENABLED, 
                null ).execute( params );
    }
}
