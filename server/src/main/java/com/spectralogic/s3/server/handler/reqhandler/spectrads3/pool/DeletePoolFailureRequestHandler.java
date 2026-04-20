/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public class DeletePoolFailureRequestHandler extends BaseDeleteBeanRequestHandler< PoolFailure >
{
    public DeletePoolFailureRequestHandler()
    {
        super( PoolFailure.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ), 
               RestDomainType.POOL_FAILURE );
    }
}
