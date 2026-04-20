/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ImportAzureTargetDirective;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseImportPublicCloudTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ImportAzureTargetRequestHandler
    extends BaseImportPublicCloudTargetRequestHandler< AzureTarget, ImportAzureTargetDirective >
{
    public ImportAzureTargetRequestHandler()
    {
        super( AzureTarget.class,
                ImportAzureTargetDirective.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                new RestfulCanHandleRequestDeterminer(
                        RestOperationType.IMPORT,
                        RestDomainType.AZURE_TARGET ) );
    }


    @Override
    protected void performImport(
            final CommandExecutionParams params,
            final ImportAzureTargetDirective directive )
    {
        params.getTargetResource().importAzureTarget( directive ).get( Timeout.DEFAULT );
    }
}
