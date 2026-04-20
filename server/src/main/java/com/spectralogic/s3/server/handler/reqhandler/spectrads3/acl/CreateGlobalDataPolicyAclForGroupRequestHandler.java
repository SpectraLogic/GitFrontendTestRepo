/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class CreateGlobalDataPolicyAclForGroupRequestHandler 
    extends BaseCreateBeanRequestHandler< DataPolicyAcl >
{
    public CreateGlobalDataPolicyAclForGroupRequestHandler()
    {
        super( DataPolicyAcl.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_POLICY_ACL,
               DefaultUserIdToUserMakingRequest.NO );
        
        registerRequiredBeanProperties( DataPolicyAcl.GROUP_ID );
    }
}
