/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.group;

import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetGroupsRequestHandler extends BaseGetBeansRequestHandler< Group >
{
    public GetGroupsRequestHandler()
    {
        super( Group.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.GROUP );
        
        registerOptionalBeanProperties( Group.BUILT_IN, NameObservable.NAME );
    }
}
