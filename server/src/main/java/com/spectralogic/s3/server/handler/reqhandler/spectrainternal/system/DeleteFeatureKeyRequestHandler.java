/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDeleteBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class DeleteFeatureKeyRequestHandler extends BaseDeleteBeanRequestHandler< FeatureKey >
{
    public DeleteFeatureKeyRequestHandler()
    {
        super( FeatureKey.class,
               new InternalAccessOnlyAuthenticationStrategy(),
               RestDomainType.FEATURE_KEY );
    }
}
