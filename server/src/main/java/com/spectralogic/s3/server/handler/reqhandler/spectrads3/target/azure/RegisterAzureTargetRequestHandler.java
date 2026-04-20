/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseRegisterPublicCloudTargetRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class RegisterAzureTargetRequestHandler
    extends BaseRegisterPublicCloudTargetRequestHandler< AzureTarget >
{
    public RegisterAzureTargetRequestHandler()
    {
        super( AzureTarget.class,
               RestDomainType.AZURE_TARGET );
    
        registerBeanProperties( 
                AzureTarget.ACCOUNT_NAME,
                AzureTarget.ACCOUNT_KEY );
    }
    
    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final AzureTarget target )
    {
        target.setId( UUID.randomUUID() );
        return params.getTargetResource().registerAzureTarget( target ).get( Timeout.DEFAULT );
    }
}
