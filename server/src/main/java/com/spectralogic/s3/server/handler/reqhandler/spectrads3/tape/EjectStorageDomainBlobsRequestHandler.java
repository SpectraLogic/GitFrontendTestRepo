/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.server.request.api.RequestParameterType;

public final class EjectStorageDomainBlobsRequestHandler extends BaseEjectStorageDomainRequestHandler
{
    public EjectStorageDomainBlobsRequestHandler()
    {
        super( true );
        registerRequiredRequestParameters( RequestParameterType.BLOBS );
        registerRequiredBeanProperties( PersistenceTarget.BUCKET_ID );
    }
}
