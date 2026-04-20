/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetSuspectBlobTapesRequestHandler extends BaseGetBeansRequestHandler< SuspectBlobTape >
{
    public GetSuspectBlobTapesRequestHandler()
    {
        super( SuspectBlobTape.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.SUSPECT_BLOB_TAPE );
        
        registerOptionalBeanProperties(
                BlobObservable.BLOB_ID,
                BlobTape.TAPE_ID );
    }
}
