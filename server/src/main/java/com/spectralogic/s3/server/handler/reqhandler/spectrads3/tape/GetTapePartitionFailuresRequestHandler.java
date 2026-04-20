/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetTapePartitionFailuresRequestHandler
    extends BaseGetBeansRequestHandler< TapePartitionFailure >
{
    public GetTapePartitionFailuresRequestHandler()
    {
        super( TapePartitionFailure.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.TAPE_PARTITION_FAILURE );
        
        registerOptionalBeanProperties( 
                TapePartitionFailure.PARTITION_ID,
                Failure.TYPE,
                ErrorMessageObservable.ERROR_MESSAGE );
    }
}
