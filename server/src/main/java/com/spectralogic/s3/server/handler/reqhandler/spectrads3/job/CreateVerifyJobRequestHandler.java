/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobbingPolicy;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
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

public final class CreateVerifyJobRequestHandler extends BaseDaoTypedRequestHandler< Job >
{
    public CreateVerifyJobRequestHandler()
    {
        super( Job.class,
               new BucketAuthorizationStrategy( 
                SystemBucketAccess.STANDARD,
                BucketAclPermission.READ, 
                AdministratorOverride.NO ),
               new RestfulCanHandleRequestDeterminer( 
                RestOperationType.START_BULK_VERIFY, 
                RestDomainType.BUCKET ) );
        
        registerOptionalBeanProperties( 
                JobObservable.PRIORITY, 
                NameObservable.NAME,
                Job.AGGREGATING );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final Bucket bucket = request.getRestRequest().getBean( 
                params.getServiceManager().getRetriever( Bucket.class ) );
        request.setBucketName( bucket.getName() );
        final Job job = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.NO );
        job.setRequestType( JobRequestType.VERIFY )
           .setChunkClientProcessingOrderGuarantee( JobChunkClientProcessingOrderGuarantee.NONE )
           .setBucketId( bucket.getId() );
        
        return new CreateJob(
                job,
                BlobbingPolicy.ENABLED, 
                null ).execute( params );
    }
}
