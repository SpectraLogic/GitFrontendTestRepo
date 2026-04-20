/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDegradedBlobsRequestHandler extends BaseGetBeansRequestHandler< DegradedBlob >
{
    public GetDegradedBlobsRequestHandler()
    {
        super( DegradedBlob.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.DEGRADED_BLOB );
        
        registerOptionalBeanProperties(
                BlobObservable.BLOB_ID,
                DegradedBlob.BUCKET_ID,
                DegradedBlob.PERSISTENCE_RULE_ID,
                DegradedBlob.DS3_REPLICATION_RULE_ID );
    }
}
