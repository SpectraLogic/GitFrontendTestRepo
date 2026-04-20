/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.shared.BaseModifyAllTargetsRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyDs3TargetRequestHandler 
    extends BaseModifyBeanRequestHandler< Ds3Target >
{
    public ModifyDs3TargetRequestHandler()
    {
        super( Ds3Target.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DS3_TARGET );
        
        registerOptionalBeanProperties(
                NameObservable.NAME,
                Ds3Target.ADMIN_AUTH_ID,
                Ds3Target.ADMIN_SECRET_KEY,
                Ds3Target.DATA_PATH_END_POINT,
                Ds3Target.DATA_PATH_HTTPS,
                Ds3Target.DATA_PATH_PORT,
                Ds3Target.DATA_PATH_PROXY,
                Ds3Target.DATA_PATH_VERIFY_CERTIFICATE,
                ReplicationTarget.DEFAULT_READ_PREFERENCE,
                ReplicationTarget.QUIESCED,
                Ds3Target.ACCESS_CONTROL_REPLICATION,
                Ds3Target.REPLICATED_USER_DEFAULT_DATA_POLICY,
                ReplicationTarget.PERMIT_GOING_OUT_OF_SYNC );
    }
    
    
    @Override
    protected void validateBeanToCommit(
            final CommandExecutionParams params,
            final Ds3Target bean,
            final Set< String > modifiedProperties )
    {
        final BeansRetriever< Ds3Target > retriever = 
                params.getServiceManager().getRetriever( Ds3Target.class );
        
        if ( modifiedProperties.contains( ReplicationTarget.QUIESCED ) ) 
        { 
            BaseModifyAllTargetsRequestHandler.validateQuiescedValueChange( retriever, bean );
        }
    }

    
    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final Ds3Target target,
            final Set< String > modifiedProperties )
    {
        params.getTargetResource().modifyDs3Target( 
                target, 
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.DEFAULT );
    }
}
