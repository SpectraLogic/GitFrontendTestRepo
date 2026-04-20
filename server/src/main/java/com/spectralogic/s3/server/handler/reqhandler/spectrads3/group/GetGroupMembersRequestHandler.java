/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import com.spectralogic.s3.common.dao.domain.ds3.GroupMember;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetGroupMembersRequestHandler extends BaseGetBeansRequestHandler < GroupMember >
{
    public GetGroupMembersRequestHandler()
    {
        super( GroupMember.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.GROUP_MEMBER );
        
        
        registerOptionalBeanProperties(
                GroupMember.MEMBER_USER_ID,
                GroupMember.MEMBER_GROUP_ID,
                GroupMember.GROUP_ID );
                
    }
}