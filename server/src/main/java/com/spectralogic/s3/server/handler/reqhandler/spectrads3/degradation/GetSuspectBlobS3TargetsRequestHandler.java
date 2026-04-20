/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetSuspectBlobS3TargetsRequestHandler
    extends BaseGetBeansRequestHandler< SuspectBlobS3Target >
{
    public GetSuspectBlobS3TargetsRequestHandler()
    {
        super( SuspectBlobS3Target.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.SUSPECT_BLOB_S3_TARGET );
        
        registerOptionalBeanProperties(
                BlobObservable.BLOB_ID,
                BlobTarget.TARGET_ID );
    }
}
