/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetStorageDomainFailuresRequestHandler
    extends BaseGetBeansRequestHandler< StorageDomainFailure >
{
    public GetStorageDomainFailuresRequestHandler()
    {
        super( StorageDomainFailure.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.STORAGE_DOMAIN_FAILURE );
        
        registerOptionalBeanProperties(
                StorageDomainFailure.STORAGE_DOMAIN_ID,
                Failure.TYPE,
                ErrorMessageObservable.ERROR_MESSAGE );
    }
}
