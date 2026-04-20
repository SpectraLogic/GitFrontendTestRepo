/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.node;

import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class ModifyNodeRequestHandler extends BaseModifyBeanRequestHandler< Node >
{
    public ModifyNodeRequestHandler()
    {
        super( Node.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ), 
               RestDomainType.NODE );
        
        registerOptionalBeanProperties( NameObservable.NAME, Node.DNS_NAME );
    }
}
