/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseImportPersistenceTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ImportPoolRequestHandler 
    extends BaseImportPersistenceTargetRequestHandler< Pool >
{
    public ImportPoolRequestHandler()
    {
        super( Pool.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.IMPORT,
                       RestDomainType.POOL ) );
    }

    
    @Override
    protected void performImport(
            final CommandExecutionParams params,
            final Pool pool,
            final ImportPersistenceTargetDirectiveRequest directive )
    {
        params.getPoolResource().importPool( pool.getId(), directive ).get( Timeout.DEFAULT );
    }
}
