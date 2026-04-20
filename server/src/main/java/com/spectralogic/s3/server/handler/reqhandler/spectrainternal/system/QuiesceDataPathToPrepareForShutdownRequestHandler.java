/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class QuiesceDataPathToPrepareForShutdownRequestHandler extends BaseRequestHandler
{
    public QuiesceDataPathToPrepareForShutdownRequestHandler()
    {
        super( new InternalAccessOnlyAuthenticationStrategy(),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_DELETE,
                       RestDomainType.DATA_PATH ) );
        
        registerOptionalRequestParameters( RequestParameterType.FORCE );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final RpcFuture< ? > plannerFuture = params.getTargetResource().quiesceAndPrepareForShutdown( 
                params.getRequest().hasRequestParameter( RequestParameterType.FORCE ) );
        final RpcFuture< ? > tapeFuture = params.getTapeResource().quiesceAndPrepareForShutdown( 
                params.getRequest().hasRequestParameter( RequestParameterType.FORCE ) );
        plannerFuture.get( Timeout.LONG );
        tapeFuture.get( Timeout.LONG );
        
        return BeanServlet.serviceDelete( params, null );
    }
}
