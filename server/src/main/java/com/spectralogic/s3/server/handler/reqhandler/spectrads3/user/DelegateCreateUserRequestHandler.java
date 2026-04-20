/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import java.util.UUID;

import org.apache.log4j.Level;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;

public final class DelegateCreateUserRequestHandler extends BaseCreateBeanRequestHandler< User >
{
    public DelegateCreateUserRequestHandler()
    {
        // users are managed by the management path, so we must delegate to it to do the actual operation
        super( User.class, 
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.USER );

        registerBeanProperties( 
                NameObservable.NAME );
        registerOptionalBeanProperties( 
                Identifiable.ID,
                User.MAX_BUCKETS,
                User.SECRET_KEY,
                User.DEFAULT_DATA_POLICY_ID );
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final User user )
    {
        final BeansRetriever< User > userRetriever = params.getServiceManager().getRetriever( User.class );
        if ( ( null != user.getId() && null != userRetriever.retrieve( user.getId() ) )
            || ( null != userRetriever.retrieve( NameObservable.NAME, user.getName() ) ) )
        {
            throw new S3RestException(
                    GenericFailure.CONFLICT,
                    "User already exists." );
        }
        
        String request = "users?name=" + user.getName() + "&username=" + user.getName() + "&role=5";
        if ( null != user.getId() )
        {
            request += "&s3_user_id=" + user.getId();
        }
        if ( null != user.getSecretKey() )
        {
            request += "&s3_secret_key=" + user.getSecretKey();
        }
        
        new SpectraViewRestRequest( RequestType.POST, request, Level.INFO )
            .addHeader( S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(), "0" )
            .addHeader( S3HeaderType.ACCEPT.getHttpHeaderName(), "application/json" )
            .run();
        
        try
        {
            return params.getServiceManager().getRetriever( User.class ).attain(
                    NameObservable.NAME, user.getName() ).getId();
        }
        catch ( final RuntimeException ex )
        {
            throw new S3RestException( 
                    GenericFailure.INTERNAL_ERROR,
                    "Management path failed to create user " + user.getName() 
                    + " (the data path request to create a user was delegated to the management path).", 
                    ex );
        }
    }
}
