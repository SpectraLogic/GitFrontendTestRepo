/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDataPolicyAclsRequestHandler extends BaseGetBeansRequestHandler< DataPolicyAcl >
{
    public GetDataPolicyAclsRequestHandler()
    {
        super( DataPolicyAcl.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.DATA_POLICY_ACL );
        
        registerOptionalBeanProperties( 
                DataPolicyAcl.DATA_POLICY_ID, 
                DataPolicyAcl.GROUP_ID, 
                UserIdObservable.USER_ID );
    }
}
