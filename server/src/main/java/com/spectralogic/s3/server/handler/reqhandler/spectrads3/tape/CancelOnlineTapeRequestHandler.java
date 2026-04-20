/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
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
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CancelOnlineTapeRequestHandler extends BaseRequestHandler
{
    public CancelOnlineTapeRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.CANCEL_ONLINE,
                       RestDomainType.TAPE ) );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Tape tape = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( Tape.class ) );
        params.getTapeResource().cancelOnlineTape( tape.getId() ).get( Timeout.LONG );

        return BeanServlet.serviceModify(
                params, 
                params.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ) );
    }
}
