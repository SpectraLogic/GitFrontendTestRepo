/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.exception.GenericFailure;

/**
 * A {@link BaseDeleteBeanRequestHandler} that only permits the bean to be deleted if the user is either (i)
 * an administrator, or (ii) the user reported via {@link UserIdObservable#USER_ID} on the bean.
 */
public abstract class UserIdObservableDeleteRequestHandler
    < T extends DatabasePersistable & UserIdObservable< ? > > extends BaseDeleteBeanRequestHandler< T >
{
    protected UserIdObservableDeleteRequestHandler(
            final RestDomainType restDomainType, 
            final Class< T > daoType )
    {
        super( daoType,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               restDomainType );
    }

    
    @Override
    protected void deleteBean( final CommandExecutionParams params, final T bean )
    {
        validate( params, bean );
        super.deleteBean( params, bean );
    }
    
    
    private void validate( final CommandExecutionParams params, final T bean )
    {
        final User user = params.getRequest().getAuthorization().getUser();
        if ( null == user )
        {
            INTERNAL_ONLY.authenticate( params );
            return;
        }
        if ( params.getGroupMembershipCache().isMember( user.getId(), BuiltInGroup.ADMINISTRATORS ) )
        {
            return;
        }
        if ( user.getId().equals( bean.getUserId() ) )
        {
            return;
        }
        
        throw new S3RestException(
                GenericFailure.FORBIDDEN,
                "User must be an administrator or the creator of the deletion target." );
    }
    
    
    private final static InternalAccessOnlyAuthenticationStrategy INTERNAL_ONLY = 
            new InternalAccessOnlyAuthenticationStrategy();
}
