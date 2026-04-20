/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public class DeleteTapeFailureRequestHandler extends BaseDeleteBeanRequestHandler< TapeFailure >
{
    public DeleteTapeFailureRequestHandler()
    {
        super( TapeFailure.class, 
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ), 
               RestDomainType.TAPE_FAILURE );
    }
}
