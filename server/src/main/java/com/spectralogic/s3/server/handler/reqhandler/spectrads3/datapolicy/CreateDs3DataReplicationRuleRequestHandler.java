/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseCreateBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateDs3DataReplicationRuleRequestHandler
    extends BaseCreateBeanRequestHandler< Ds3DataReplicationRule >
{
    public CreateDs3DataReplicationRuleRequestHandler()
    {
        super( Ds3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.DS3_DATA_REPLICATION_RULE );

        registerBeanProperties(
                DataPlacement.DATA_POLICY_ID, 
                DataReplicationRule.TARGET_ID,
                DataReplicationRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES,
                Ds3DataReplicationRule.TARGET_DATA_POLICY );
    }


    @Override
    protected void prepareBeanForCreation( final Ds3DataReplicationRule rule )
    {
        rule.setId( UUID.randomUUID() );
        rule.setState( DataPlacementRuleState.NORMAL );
    }


    @Override
    protected UUID createBean( final CommandExecutionParams params, final Ds3DataReplicationRule bean )
    {
        return params.getDataPolicyResource().createDs3DataReplicationRule( bean ).get( Timeout.LONG );
    }
}
