/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public final class GetUserRequestHandler extends BaseGetBeanRequestHandler< User >
{
    public GetUserRequestHandler()
    {
        super( User.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.USER );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final User user;
        try
        {
            user = request.getRestRequest().getBean(
                    params.getServiceManager().getRetriever( User.class ) );
        }
        catch ( final RuntimeException ex )
        {
            new DefaultPublicExposureAuthenticationStrategy( 
                    RequiredAuthentication.ADMINISTRATOR ).authenticate( params );
            throw ex;
        }
        
        if ( null != request.getAuthorization().getUser() 
                && !user.getId().equals( request.getAuthorization().getUser().getId() ) )
        {
            new DefaultPublicExposureAuthenticationStrategy( 
                    RequiredAuthentication.ADMINISTRATOR ).authenticate( params );
        }
        
        return super.handleRequestInternal( request, params );
    }
}
