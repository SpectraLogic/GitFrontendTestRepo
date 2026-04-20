/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
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

public final class ConvertStorageDomainToDs3TargetRequestHandler extends BaseRequestHandler
{
    public ConvertStorageDomainToDs3TargetRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.MODIFY, 
                       RestDomainType.STORAGE_DOMAIN ) );
        registerRequiredRequestParameters( RequestParameterType.CONVERT_TO_DS3_TARGET );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final StorageDomain storageDomain = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( StorageDomain.class ) );
        
        params.getDataPolicyResource().convertStorageDomainToDs3Target(
                storageDomain.getId(),
                request.getRequestParameter( RequestParameterType.CONVERT_TO_DS3_TARGET ).getUuid() ).get( 
                        Timeout.LONG );
        
        return BeanServlet.serviceDelete( params, null );
    }
}
