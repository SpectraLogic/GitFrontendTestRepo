/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class VerifyS3TargetRequestHandler extends BaseRequestHandler
{
    public VerifyS3TargetRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer( 
                       RestOperationType.VERIFY, 
                       RestDomainType.S3_TARGET ) );
        registerOptionalRequestParameters(
                RequestParameterType.FULL_DETAILS );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansRetriever< S3Target > retriever =
                params.getServiceManager().getRetriever( S3Target.class );
        final UUID targetId = request.getRestRequest().getBean( retriever ).getId();
        
        params.getTargetResource().verifyS3Target( 
                targetId,
                request.hasRequestParameter( RequestParameterType.FULL_DETAILS ) ).get( Timeout.DEFAULT );
        
        return BeanServlet.serviceGet( params, retriever.attain( targetId ) );
    }
}
