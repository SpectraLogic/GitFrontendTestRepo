/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.user;

import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.ds3.UserService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.security.PasswordGenerator;

public final class CreateUserRequestHandler extends BaseCreateBeanRequestHandler< User >
{
    public CreateUserRequestHandler()
    {
        // users are managed by the management path, so only allow internal requests to create them
        super( User.class, 
               new InternalAccessOnlyAuthenticationStrategy(),
               RestDomainType.USER_INTERNAL );
        
        registerBeanProperties( 
                User.DEFAULT_DATA_POLICY_ID, 
                NameObservable.NAME );
        registerOptionalBeanProperties( 
                Identifiable.ID,
                User.MAX_BUCKETS,
                User.SECRET_KEY );
        registerOptionalRequestParameters(
                RequestParameterType.FORCE );
    }
    
    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final User user )
    {
        if ( null == user.getSecretKey() )
        {
            user.setSecretKey( PasswordGenerator.generate( SECRET_KEY_LENGTH ) );
        }
        user.setAuthId( Base64.encodeBase64String( user.getName().getBytes() ) );
        PasswordGenerator.verify( user.getSecretKey() );
        if ( 0 > user.getMaxBuckets() )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "Maximum user bucket count cannot be less than 0." );
        }
    
        try
        {
            final boolean force = params.getRequest().hasRequestParameter( RequestParameterType.FORCE );
            if ( null == user.getId() )
            {
                user.setId( UUID.randomUUID() );
            }
            if ( 0 >= params.getServiceManager().getRetriever( Ds3Target.class ).getCount() )
            {
                params.getServiceManager().getService( UserService.class ).create( user );
            }
            else
            {
                try
                {
                    params.getTargetResource().createUser( 
                            null != params.getRequest().getHttpRequest().getHeader( 
                                    S3HeaderType.REPLICATION_SOURCE_IDENTIFIER ),
                            user ).get( Timeout.DEFAULT );
                }
                catch ( RuntimeException ex )
                {
                    if ( force )
                    {
                        params.getServiceManager().getService( UserService.class ).create( user );
                        LOG.warn( "Failed to create user " + user.getName() + " via target resource."
                                + " Request is forced, so created user locally anyway.", ex );
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }
            return user.getId();
        }
        finally
        {
            params.getGroupMembershipCache().invalidate();
        }
    }


    public final static int SECRET_KEY_LENGTH = 8;
}