/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.ReservedTaskType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeRole;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.server.exception.S3RestException;
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
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

import java.util.UUID;

public final class CreateDriveDumpRequestHandler extends BaseRequestHandler
{
    public CreateDriveDumpRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.DUMP,
                       RestDomainType.TAPE_DRIVE ) );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final TapeDrive drive = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( TapeDrive.class ) );

        params.getTapeResource().driveDump( drive.getId() ).get( Timeout.LONG );

        return BeanServlet.serviceModify(
                params, 
                params.getServiceManager().getRetriever( TapeDrive.class ).attain( drive.getId() ) );
    }
}
