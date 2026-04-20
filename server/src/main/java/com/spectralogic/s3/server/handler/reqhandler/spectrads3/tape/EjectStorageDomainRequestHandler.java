/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;


public final class EjectStorageDomainRequestHandler extends BaseEjectStorageDomainRequestHandler
{
    public EjectStorageDomainRequestHandler()
    {
        super( false );
        registerOptionalBeanProperties( PersistenceTarget.BUCKET_ID );
    }
}
