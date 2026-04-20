/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class GetDs3TargetDataPoliciesRequestHandler extends BaseDaoTypedRequestHandler< DataPolicy >
{
    public GetDs3TargetDataPoliciesRequestHandler()
    {
        super( DataPolicy.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.SHOW, 
                       RestDomainType.DS3_TARGET_DATA_POLICIES ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Ds3Target target = request.getRestRequest().getBean( 
                params.getServiceManager().getRetriever( Ds3Target.class ) );
        final Set< DataPolicy > retval = BeanUtils.sort( CollectionFactory.toSet( 
                params.getTargetResource().getDataPolicies( 
                        target.getId() ).get( Timeout.DEFAULT ).getDataPolicies() ) );
        return BeanServlet.serviceGet( params, CollectionFactory.toArray( DataPolicy.class, retval ) );
    }
}
