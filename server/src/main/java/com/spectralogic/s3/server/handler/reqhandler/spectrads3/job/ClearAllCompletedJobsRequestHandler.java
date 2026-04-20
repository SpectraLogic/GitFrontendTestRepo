/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.service.ds3.CompletedJobService;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public final class ClearAllCompletedJobsRequestHandler extends BaseRequestHandler
{
    public ClearAllCompletedJobsRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer( 
                RestActionType.BULK_DELETE,
                RestDomainType.COMPLETED_JOB ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        params.getServiceManager().getService( CompletedJobService.class ).deleteAll();
        return BeanServlet.serviceDelete( params, null );
    }
}
