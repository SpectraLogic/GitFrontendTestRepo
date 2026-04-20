/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.JobChunkApiBean;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.job.shared.JobResponseBuilder;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;

public final class GetJobChunkRequestHandler extends BaseRequestHandler
{
    public GetJobChunkRequestHandler()
    {
        super( new BucketAuthorizationStrategy( 
                SystemBucketAccess.STANDARD,
                BucketAclPermission.JOB, 
                AdministratorOverride.YES ),
               new RestfulCanHandleRequestDeterminer(
                RestActionType.SHOW,
                RestDomainType.JOB_CHUNK ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final BeansRetrieverManager brm = params.getServiceManager();
        final JobEntry daoChunk = request.getRestRequest().getBean( brm.getRetriever( JobEntry.class ) );
        final Set< Blob > blobs = 
                brm.getRetriever( Blob.class ).retrieveAll(
                        Require.exists(
                                JobEntry.class,
                                BlobObservable.BLOB_ID, 
                                Require.beanPropertyEquals( 
                                        JobEntry.ID,
                                        daoChunk.getId() ) ) ).toSet();
        final JobChunkApiBean chunk = new JobResponseBuilder( 
                request.getRestRequest().getId( params.getServiceManager().getRetriever( Job.class ) ),
                params ).buildChunk(
                        daoChunk,
                        blobs, 
                        null );
        return BeanServlet.serviceGet( params, chunk );
    }
}
