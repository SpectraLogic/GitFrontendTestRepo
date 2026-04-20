/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyPoolRequestHandler extends BaseModifyBeanRequestHandler< Pool >
{
    public ModifyPoolRequestHandler()
    {
        super( Pool.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.POOL );
        
        registerOptionalBeanProperties( Pool.PARTITION_ID, Pool.QUIESCED );
    }

    
    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final Pool bean,
            final Set< String > modifiedProperties )
    {
        if ( modifiedProperties.contains( TapePartition.QUIESCED ) ) 
        { 
            final PoolService poolService = params.getServiceManager().getService( PoolService.class );
            validateQuiescedValueChange( poolService, bean );
            poolService.update( bean, Pool.QUIESCED );
        }
        params.getDataPolicyResource().modifyPool( bean.getId(), bean.getPartitionId() ).get( Timeout.LONG );
    }
    
    
    protected static void validateQuiescedValueChange( 
            final BeansRetriever< Pool > poolRetriever, 
            final Pool bean ) 
    {
        final Pool pool = 
                poolRetriever.attain( bean.getId() );
        
        if ( pool.getQuiesced().equals( bean.getQuiesced() )
                || Quiesced.NO == bean.getQuiesced()
                || Quiesced.PENDING == bean.getQuiesced() )
        {
            return;
        }
        
        throw new FailureTypeObservableException( 
                GenericFailure.BAD_REQUEST,
                "It is illegal to transist the quiesced state from " + pool.getQuiesced()
                + " to " + bean.getQuiesced() + "." );
    }
}
