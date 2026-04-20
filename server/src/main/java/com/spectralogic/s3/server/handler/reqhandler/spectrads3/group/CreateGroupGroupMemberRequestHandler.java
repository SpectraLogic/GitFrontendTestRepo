/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.platform.security.GroupMembershipCalculator;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;

public final class CreateGroupGroupMemberRequestHandler extends BaseCreateBeanRequestHandler< GroupMember >
{
    public CreateGroupGroupMemberRequestHandler()
    {
        super( GroupMember.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.GROUP_MEMBER );
        
        registerRequiredBeanProperties( 
                GroupMember.GROUP_ID,
                GroupMember.MEMBER_GROUP_ID );
    }

    
    @Override
    protected UUID createBean( CommandExecutionParams params, GroupMember bean )
    {
        if ( bean.getGroupId().equals(
                params.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                        BuiltInGroup.EVERYONE ).getId() ) )
        {
            throw new S3RestException( 
                    GenericFailure.FORBIDDEN,
                    "Built-in group " + BuiltInGroup.EVERYONE + " cannot be modified." );
        }
        new GroupMembershipCalculator( params.getServiceManager() ).addGroupMember( bean ).calculate();
        
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
