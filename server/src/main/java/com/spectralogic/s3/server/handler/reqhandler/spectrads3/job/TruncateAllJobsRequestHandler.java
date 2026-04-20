/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public class TruncateAllJobsRequestHandler extends BaseDaoTypedRequestHandler< Job >
{
    public TruncateAllJobsRequestHandler()
    {
        this( RestDomainType.JOB );
    }
    
    
    protected TruncateAllJobsRequestHandler( final RestDomainType domain )
    {
        super( Job.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer( 
                        RestActionType.BULK_DELETE,
                        domain ) );

        registerOptionalBeanProperties( JobObservable.REQUEST_TYPE, JobObservable.BUCKET_ID );
    }


    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        return new CancelAllJobsRequestHandler().handleRequestInternal( request, params );
    }
}
