/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.node;

import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetNodeRequestHandler extends BaseGetBeanRequestHandler< Node >
{
    public GetNodeRequestHandler()
    {
        super( Node.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.NONE ),
               RestDomainType.NODE );
    }
}
