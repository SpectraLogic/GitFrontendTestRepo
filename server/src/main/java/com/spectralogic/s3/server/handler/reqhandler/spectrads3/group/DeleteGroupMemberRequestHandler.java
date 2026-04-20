/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;

public final class DeleteGroupMemberRequestHandler extends BaseDeleteBeanRequestHandler< GroupMember >
{
    public DeleteGroupMemberRequestHandler()
    {
        super( GroupMember.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.GROUP_MEMBER );
    }

    
    @Override
    protected void deleteBean( final CommandExecutionParams params, final GroupMember member )
    {
        final Group group = 
                params.getServiceManager().getRetriever( Group.class ).attain( member.getGroupId() );
        if ( group.getId().equals(
                params.getServiceManager().getService( GroupService.class ).getBuiltInGroup(
                        BuiltInGroup.EVERYONE ).getId() ) )
        {
            throw new S3RestException( 
                    GenericFailure.FORBIDDEN,
                    "Built-in group " + BuiltInGroup.EVERYONE + " cannot be modified." );
        }

        super.deleteBean( params, member );
        params.getGroupMembershipCache().invalidate();
    }
}
