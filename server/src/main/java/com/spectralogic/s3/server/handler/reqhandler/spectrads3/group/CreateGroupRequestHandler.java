/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateGroupRequestHandler extends BaseCreateBeanRequestHandler< Group >
{
    public CreateGroupRequestHandler()
    {
        super( Group.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.GROUP );
        
        registerBeanProperties( NameObservable.NAME );
    }

    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final Group bean )
    {
        try
        {
            return super.createBean( params, bean );
        }
        finally
        {
            params.getGroupMembershipCache().invalidate();
        }
    }
}