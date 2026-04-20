/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetAzureTargetsRequestHandler
    extends BaseGetBeansRequestHandler< AzureTarget >
{
    public GetAzureTargetsRequestHandler()
    {
        super( AzureTarget.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.AZURE_TARGET );
    
        registerOptionalBeanProperties(
                NameObservable.NAME,
                AzureTarget.ACCOUNT_NAME,
                PublicCloudReplicationTarget.HTTPS,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.QUIESCED,
                ReplicationTarget.STATE,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }

    @Override
    protected AzureTarget performCustomPopulationWork(DS3Request request, CommandExecutionParams params, AzureTarget bean) {
        bean.setAccountKey(null);
        return bean;
    }
}
