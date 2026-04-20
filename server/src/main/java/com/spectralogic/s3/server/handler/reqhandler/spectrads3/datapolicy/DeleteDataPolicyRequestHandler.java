/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteDataPolicyRequestHandler extends BaseDeleteBeanRequestHandler< DataPolicy >
{
    public DeleteDataPolicyRequestHandler()
    {
        super( DataPolicy.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_POLICY );
    }
}
