/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class RegisterDs3TargetRequestHandler 
    extends BaseCreateBeanRequestHandler< Ds3Target >
{
    public RegisterDs3TargetRequestHandler()
    {
        super( Ds3Target.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DS3_TARGET );
        
        registerBeanProperties( 
                NameObservable.NAME,
                Ds3Target.ADMIN_AUTH_ID,
                Ds3Target.ADMIN_SECRET_KEY,
                Ds3Target.DATA_PATH_END_POINT,
                Ds3Target.DATA_PATH_HTTPS,
                Ds3Target.DATA_PATH_PORT,
                Ds3Target.DATA_PATH_PROXY,
                Ds3Target.DATA_PATH_VERIFY_CERTIFICATE,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                Ds3Target.ACCESS_CONTROL_REPLICATION,
                Ds3Target.REPLICATED_USER_DEFAULT_DATA_POLICY,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }

    
    @Override
    protected UUID createBean( final CommandExecutionParams params, final Ds3Target target )
    {
        target.setId( UUID.randomUUID() );
        return params.getTargetResource().registerDs3Target( target ).get( Timeout.DEFAULT );
    }
}
