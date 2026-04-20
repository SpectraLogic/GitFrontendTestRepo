/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.user;

import java.util.Set;

import org.apache.commons.codec.binary.Base64;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.security.PasswordGenerator;

public final class ModifyUserRequestHandler extends BaseModifyBeanRequestHandler< User >
{
    public ModifyUserRequestHandler()
    {
        super( User.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.USER );
        
        registerOptionalBeanProperties( 
                NameObservable.NAME,
                User.DEFAULT_DATA_POLICY_ID,
                User.MAX_BUCKETS,
                User.SECRET_KEY );
    }
    

    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final User user,
            final Set< String > modifiedProperties )
    {
        if ( modifiedProperties.contains( NameObservable.NAME ) )
        {
            // user names are managed by the management path, so only allow internal requests to modify them
            new InternalAccessOnlyAuthenticationStrategy().authenticate( params );
            modifiedProperties.add( User.AUTH_ID );
            user.setAuthId( Base64.encodeBase64String( user.getName().getBytes() ) );
        }
        if ( modifiedProperties.contains( User.SECRET_KEY ) )
        {
            PasswordGenerator.verify( user.getSecretKey() );
        }
        if ( modifiedProperties.contains( User.MAX_BUCKETS ) )
        {
            if ( 0 > user.getMaxBuckets() )
            {
                throw new S3RestException( GenericFailure.BAD_REQUEST,
                        "Maximum user bucket count cannot be less than 0." );
            }
        }
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final User user,
            final Set< String > modifiedProperties )
    {
        params.getTargetResource().modifyUser(
                null != params.getRequest().getHttpRequest().getHeader( 
                        S3HeaderType.REPLICATION_SOURCE_IDENTIFIER ),
                user,
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.DEFAULT );
    }
}
