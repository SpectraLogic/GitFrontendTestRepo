/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class ResetInstanceIdentifierRequestHandler 
    extends BaseModifyBeanRequestHandler< DataPathBackend >
{
    public ResetInstanceIdentifierRequestHandler()
    {
        super( DataPathBackend.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.INSTANCE_IDENTIFIER );
    }

    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final DataPathBackend bean,
            final Set< String > modifiedProperties )
    {
        final UUID oldId = bean.getInstanceId();
        final UUID newId = UUID.randomUUID();
        
        LOG.warn( "The instance ID is being changed from " + oldId + " to " + newId + "." );
        
        modifiedProperties.add( DataPathBackend.INSTANCE_ID );
        bean.setInstanceId( newId );
    }
}
