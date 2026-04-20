/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateDataPersistenceRuleRequestHandler 
    extends BaseCreateBeanRequestHandler< DataPersistenceRule >
{
    public CreateDataPersistenceRuleRequestHandler()
    {
        super( DataPersistenceRule.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               RestDomainType.DATA_PERSISTENCE_RULE );
        
        registerBeanProperties(
                DataPersistenceRule.ISOLATION_LEVEL,
                DataPlacement.DATA_POLICY_ID, 
                DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                DataPersistenceRule.STORAGE_DOMAIN_ID,
                DataPersistenceRule.TYPE );
    }
    
    
    @Override
    protected void prepareBeanForCreation( final DataPersistenceRule rule )
    {
        validate( rule );
        rule.setId( UUID.randomUUID() );
        rule.setState( DataPlacementRuleState.NORMAL );
    }
    
    
    static void validate( final DataPersistenceRule rule )
    {
        final boolean retentionApplies = ( DataPersistenceRuleType.TEMPORARY == rule.getType() );
        if ( retentionApplies )
        {
            if ( null == rule.getMinimumDaysToRetain() )
            {
                throw new S3RestException( 
                        GenericFailure.CONFLICT, 
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN 
                        + " must be specified for " + rule.getType() + " data persistence rules." );
            }
            if ( 0 > rule.getMinimumDaysToRetain().intValue() )
            {
                throw new S3RestException( 
                        GenericFailure.CONFLICT, 
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN 
                        + " cannot be negative." );
            }
        }
        if ( !retentionApplies && null != rule.getMinimumDaysToRetain() )
        {
            throw new S3RestException( 
                    GenericFailure.CONFLICT, 
                    DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN + " cannot be specified for " + rule.getType() 
                    + " data persistence rules." );
        }
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final DataPersistenceRule bean )
    {
        return params.getDataPolicyResource().createDataPersistenceRule( bean ).get( Timeout.LONG );
    }
}
