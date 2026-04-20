/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.auth;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.lang.Validations;

/**
 * Default authentication strategy for requests that are publicly exposed to clients.
 */
public class DefaultPublicExposureAuthenticationStrategy 
    extends InternalAccessOnlyAuthenticationStrategy
    implements AuthenticationStrategy
{
    public enum RequiredAuthentication
    {
        ADMINISTRATOR,
        TAPE_ADMIN,
        USER,
        NONE
    }
    
    
    public DefaultPublicExposureAuthenticationStrategy( final RequiredAuthentication requiresAuthentication )
    {
        m_requiresValidUser = requiresAuthentication;
        Validations.verifyNotNull( "Requires authentication", requiresAuthentication );
    }
    
    
    /**
     * @param commandExecutionParams
     */
    @Override
    public void authenticate( final CommandExecutionParams commandExecutionParams )
    {
        if ( isRequestInternal( commandExecutionParams.getRequest().getHttpRequest() ) )
        {
            return;
        }
        
        switch ( m_requiresValidUser )
        {
            case NONE:
                // do nothing
                break;
            case USER:
                verifyUserAccount( commandExecutionParams, null );
                break;
            case TAPE_ADMIN:
                verifyUserAccount( commandExecutionParams, BuiltInGroup.TAPE_ADMINS );
                break;
            case ADMINISTRATOR:
                verifyUserAccount( commandExecutionParams, BuiltInGroup.ADMINISTRATORS );
                break;
            default:
                throw new UnsupportedOperationException( "No code for " + m_requiresValidUser + "." );
        }
    }
    
    
    private static void verifyUserAccount(
            final CommandExecutionParams params, 
            final BuiltInGroup requiredGroupMembership )
    {
        final User user = params.getRequest().getAuthorization().getUser();
        if ( null == user || null == user.getName() )
        {
            throw new S3RestException(
                    AWSFailure.ACCESS_DENIED,
                    "Authorization is required for this request." );
        }
        
        if ( null != requiredGroupMembership )
        {
            if ( !params.getGroupMembershipCache().isMember( user.getId(), BuiltInGroup.ADMINISTRATORS )
                    && !params.getGroupMembershipCache().isMember( user.getId(), requiredGroupMembership ) )
            {
                throw new S3RestException(
                        AWSFailure.ACCESS_DENIED,
                        "User " + user.getName() + " must be a member of the "
                        + requiredGroupMembership.getName() 
                        + " group to perform the requested operation." );
            }
        }
    }
    
    
    private final RequiredAuthentication m_requiresValidUser;
}
