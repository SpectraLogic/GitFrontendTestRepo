/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskInformation;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CompactPoolRequestHandler extends BaseDaoTypedRequestHandler< BlobStoreTaskInformation >
{
    public CompactPoolRequestHandler()
    {
        super( BlobStoreTaskInformation.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.COMPACT,
                       RestDomainType.POOL ) );
        
        registerOptionalBeanProperties( BlobStoreTask.PRIORITY );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BlobStoreTaskInformation priority = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.YES );
        final Pool pool = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( Pool.class ) );
        params.getPoolResource().compactPool( pool.getId(), priority.getPriority() ).get( Timeout.LONG );

        return BeanServlet.serviceModify(
                params, 
                params.getServiceManager().getRetriever( Pool.class ).attain( pool.getId() ) );
    }
}
