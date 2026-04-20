/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetPoolFailuresRequestHandler extends BaseGetBeansRequestHandler< PoolFailure >
{
    public GetPoolFailuresRequestHandler()
    {
        super( PoolFailure.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.POOL_FAILURE );
        
        registerOptionalBeanProperties( 
                PoolFailure.POOL_ID, 
                Failure.TYPE,
                ErrorMessageObservable.ERROR_MESSAGE );
    }
}
