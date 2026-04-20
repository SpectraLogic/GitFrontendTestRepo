/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDs3TargetsRequestHandler
    extends BaseGetBeansRequestHandler< Ds3Target >
{
    public GetDs3TargetsRequestHandler()
    {
        super( Ds3Target.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.DS3_TARGET );

        registerOptionalBeanProperties(
                NameObservable.NAME,
                Ds3Target.ADMIN_AUTH_ID,
                Ds3Target.DATA_PATH_END_POINT,
                Ds3Target.DATA_PATH_HTTPS,
                Ds3Target.DATA_PATH_PORT,
                Ds3Target.DATA_PATH_PROXY,
                Ds3Target.DATA_PATH_VERIFY_CERTIFICATE,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.QUIESCED,
                ReplicationTarget.STATE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }
}
