/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.service.target.SuspectBlobS3TargetService;
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
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class ClearSuspectBlobS3TargetsRequestHandler extends BaseRequestHandler
{
    public ClearSuspectBlobS3TargetsRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.BULK_DELETE, 
                       RestDomainType.SUSPECT_BLOB_S3_TARGET ) );
        registerOptionalRequestParameters( RequestParameterType.FORCE );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Set< UUID > ids = SuspectBlobUtil.extractIds( request );
        final BeansServiceManager transaction = params.getServiceManager().startTransaction();
        try
        {
            transaction.getService( SuspectBlobS3TargetService.class ).delete( ids );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        return BeanServlet.serviceDelete( params, null );
    }
}
