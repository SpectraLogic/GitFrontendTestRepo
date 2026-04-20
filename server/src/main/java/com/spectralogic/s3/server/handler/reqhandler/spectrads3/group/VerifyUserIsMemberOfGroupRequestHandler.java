/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;

public final class VerifyUserIsMemberOfGroupRequestHandler extends BaseDaoTypedRequestHandler< Bucket >
{
    public VerifyUserIsMemberOfGroupRequestHandler()
    {
        super( Bucket.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.VERIFY,
                       RestDomainType.GROUP ) );
        registerOptionalBeanProperties( UserIdObservable.USER_ID );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Group group = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( Group.class ) );
        final UUID userId = ( request.getBeanPropertyValueMapFromRequestParameters().containsKey(
                UserIdObservable.USER_ID ) ) ?
                        getBeanSpecifiedViaQueryParameters( 
                                params, AutoPopulatePropertiesWithDefaults.YES ).getUserId() 
                        : request.getAuthorization().getUser().getId();
        if ( !userId.equals( request.getAuthorization().getUser().getId() ) )
        {
            new DefaultPublicExposureAuthenticationStrategy( 
                    RequiredAuthentication.ADMINISTRATOR ).authenticate( params );
        }
        
        return BeanServlet.serviceModify( 
                params,
                params.getGroupMembershipCache().getGroups( userId ).contains( group.getId() ) ? 
                        group 
                        : null );
    }
}
