/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner;

import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.util.UUID;

public final class GetDataPlannerBlobStoreTasksRequestHandler extends BaseRequestHandler
{
    public GetDataPlannerBlobStoreTasksRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer( RestActionType.LIST, RestDomainType.BLOB_STORE_TASK ) );
        
        registerOptionalRequestParameters( RequestParameterType.FULL_DETAILS, RequestParameterType.JOB );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BlobStoreTaskState [] states =
                ( request.hasRequestParameter( RequestParameterType.FULL_DETAILS ) ) ?
                        BlobStoreTaskState.values()
                        : new BlobStoreTaskState [] { 
                                BlobStoreTaskState.IN_PROGRESS,
                                BlobStoreTaskState.PENDING_EXECUTION };
        UUID jobId = null;
        if ( request.hasRequestParameter( RequestParameterType.JOB ) ) {
            jobId = request.getRequestParameter( RequestParameterType.JOB ).getUuid();
        }
        return BeanServlet.serviceGet(
                params, 
                params.getPlannerResource().getBlobStoreTasksForJob( jobId, states ).get( Timeout.LONG ) );
    }
}
