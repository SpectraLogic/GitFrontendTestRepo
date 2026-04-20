/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;

public final class ModifyGroupRequestHandler extends BaseModifyBeanRequestHandler< Group >
{
    public ModifyGroupRequestHandler()
    {
        super( Group.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ), 
               RestDomainType.GROUP );
        
        registerOptionalBeanProperties( NameObservable.NAME );
    }

    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final Group bean,
            final Set< String > modifiedProperties )
    {
        if ( bean.isBuiltIn() )
        {
            throw new S3RestException( GenericFailure.FORBIDDEN, "Built-in groups cannot be modified." );
        }
    }
}
