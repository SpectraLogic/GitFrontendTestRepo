/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class EjectAllTapesRequestHandler extends BaseDaoTypedRequestHandler< Tape >
{
    public EjectAllTapesRequestHandler()
    {
        super( Tape.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_MODIFY,
                       RestOperationType.EJECT,
                       RestDomainType.TAPE ) );
        
        registerOptionalBeanProperties(
                Tape.EJECT_LABEL, Tape.EJECT_LOCATION );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Tape queryParams = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.NO );
        final TapeFailuresInformation rpcResponse = 
                params.getTapeResource().ejectTape( 
                        null,
                        queryParams.getEjectLabel(),
                        queryParams.getEjectLocation() ).get( Timeout.LONG );
        if ( 0 == rpcResponse.getFailures().length )
        {
            return BeanServlet.serviceModify( params, null );
        }
        return BeanServlet.serviceRequest( 
                params,
                207, // multiple statuses
                new TapeFailuresResponseBuilder( rpcResponse, params ).build() );
    }
}
