/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import org.apache.log4j.Level;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public final class DelegateDeleteUserRequestHandler extends BaseDeleteBeanRequestHandler< User >
{
    public DelegateDeleteUserRequestHandler()
    {
        // users are managed by the management path, so we must delegate to it to do the actual operation
        super( User.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.USER );
    }

    
    @Override
    protected void deleteBean( final CommandExecutionParams params, final User user )
    {
        // Encode username in UTF-8 so that it will work as a query parameter even if it contains illegal charaacters.
        final String username;
        try {
            username = URLEncoder.encode(user.getName(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException( "Failed to encode username in UTF-8.", e);
        }
        new SpectraViewRestRequest( RequestType.DELETE, "users?username=" + username, Level.INFO )
            .addHeader( S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(), "0" )
            .addHeader( S3HeaderType.ACCEPT.getHttpHeaderName(), "application/json" )
            .run();
        
        if ( null != params.getServiceManager().getRetriever( User.class ).retrieve( user.getId() ) )
        {
            throw new S3RestException(
                    GenericFailure.INTERNAL_ERROR, 
                    "Management path failed to delete user " + user.getName() 
                    + " (the data path request to delete the user was delegated to the management path)." );
        }
    }
}
