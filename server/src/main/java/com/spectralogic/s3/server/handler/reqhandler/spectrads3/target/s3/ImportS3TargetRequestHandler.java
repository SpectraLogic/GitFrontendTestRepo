/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.target.ImportS3TargetDirective;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseImportPublicCloudTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ImportS3TargetRequestHandler
    extends BaseImportPublicCloudTargetRequestHandler< S3Target, ImportS3TargetDirective >
{
    public ImportS3TargetRequestHandler()
    {
        super( S3Target.class,
                ImportS3TargetDirective.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                new RestfulCanHandleRequestDeterminer(
                        RestOperationType.IMPORT,
                        RestDomainType.S3_TARGET ) );
    }
    
    
    @Override
    protected void performImport(
            final CommandExecutionParams params,
            final ImportS3TargetDirective directive )
    {
        params.getTargetResource().importS3Target( directive ).get( Timeout.DEFAULT );
    }
}
