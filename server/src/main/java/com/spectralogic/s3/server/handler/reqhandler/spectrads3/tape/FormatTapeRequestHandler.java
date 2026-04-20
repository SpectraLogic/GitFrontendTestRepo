/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
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
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class FormatTapeRequestHandler extends BaseRequestHandler
{
    public FormatTapeRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.FORMAT,
                       RestDomainType.TAPE ) );
        
        registerOptionalRequestParameters( RequestParameterType.FORCE );
        registerOptionalRequestParameters( RequestParameterType.CHARACTERIZE );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Tape tape = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( Tape.class ) );
        params.getTapeResource().formatTape( 
                tape.getId(), 
                request.hasRequestParameter( RequestParameterType.FORCE ),
                request.hasRequestParameter( RequestParameterType.CHARACTERIZE ) ).get( Timeout.LONG );

        return BeanServlet.serviceModify(
                params, 
                params.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ) );
    }
}
