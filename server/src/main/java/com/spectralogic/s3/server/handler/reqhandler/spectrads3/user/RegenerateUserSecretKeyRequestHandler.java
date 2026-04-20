/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrainternal.user.CreateUserRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.security.PasswordGenerator;

public final class RegenerateUserSecretKeyRequestHandler extends BaseRequestHandler
{
    public RegenerateUserSecretKeyRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.REGENERATE_SECRET_KEY,
                       RestDomainType.USER ) );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final User user = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( User.class ) );
        if ( null == request.getAuthorization().getUser() 
                || !user.getId().equals( request.getAuthorization().getUser().getId() ) )
        {
            new DefaultPublicExposureAuthenticationStrategy( 
                    RequiredAuthentication.ADMINISTRATOR ).authenticate( params );
        }
        
        user.setSecretKey( PasswordGenerator.generate( CreateUserRequestHandler.SECRET_KEY_LENGTH ) );
        params.getTargetResource().modifyUser( 
                null != params.getRequest().getHttpRequest().getHeader( 
                        S3HeaderType.REPLICATION_SOURCE_IDENTIFIER ),
                user, 
                new String [] { User.SECRET_KEY } ).get( Timeout.DEFAULT );
        return BeanServlet.serviceModify( params, user );
    }
}
