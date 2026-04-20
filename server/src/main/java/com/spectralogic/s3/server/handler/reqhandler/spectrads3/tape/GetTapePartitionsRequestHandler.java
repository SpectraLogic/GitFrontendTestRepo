/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public class GetTapePartitionsRequestHandler extends BaseGetBeansRequestHandler< TapePartition >
{
    public GetTapePartitionsRequestHandler()
    {
        super( TapePartition.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.TAPE_PARTITION );
        
        registerOptionalBeanProperties(
                TapePartition.IMPORT_EXPORT_CONFIGURATION,
                TapePartition.LIBRARY_ID,
                TapePartition.QUIESCED,
                TapePartition.STATE,
                NameObservable.NAME,
                SerialNumberObservable.SERIAL_NUMBER );
    }
}
