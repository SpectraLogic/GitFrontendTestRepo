/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseImportPersistenceTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ImportTapeRequestHandler extends BaseImportPersistenceTargetRequestHandler< Tape >
{
    public ImportTapeRequestHandler()
    {
        super( Tape.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.IMPORT,
                       RestDomainType.TAPE ) );
    }

    
    @Override
    protected void performImport(
            final CommandExecutionParams params,
            final Tape tape,
            final ImportPersistenceTargetDirectiveRequest directive )
    {
        params.getTapeResource().importTape( tape.getId(), directive ).get( Timeout.DEFAULT );
    }
}
