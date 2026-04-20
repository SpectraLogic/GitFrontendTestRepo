/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyDs3DataReplicationRuleRequestHandler
    extends BaseModifyBeanRequestHandler< Ds3DataReplicationRule >
{
    public ModifyDs3DataReplicationRuleRequestHandler()
    {
        super( Ds3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.DS3_DATA_REPLICATION_RULE );

        registerOptionalBeanProperties(
                DataPersistenceRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES,
                Ds3DataReplicationRule.TARGET_DATA_POLICY );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final Ds3DataReplicationRule rule,
            final Set< String > modifiedProperties )
    {
        params.getDataPolicyResource().modifyDs3DataReplicationRule(
                rule,
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
    }
}
