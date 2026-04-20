/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetS3TargetsRequestHandler
    extends BaseGetBeansRequestHandler< S3Target >
{
    public GetS3TargetsRequestHandler()
    {
        super( S3Target.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.S3_TARGET );
    
        registerOptionalBeanProperties(
                NameObservable.NAME,
                S3Target.REGION,
                S3Target.ACCESS_KEY,
                S3Target.DATA_PATH_END_POINT,
                PublicCloudReplicationTarget.HTTPS,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.QUIESCED,
                ReplicationTarget.STATE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC,
                PublicCloudReplicationTarget.NAMING_MODE  );
    }

    @Override
    protected S3Target performCustomPopulationWork(DS3Request request, CommandExecutionParams params, S3Target bean) {
        bean.setSecretKey(null);
        return bean;
    }
}
