/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseModifyBeanRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class ModifyAzureDataReplicationRuleRequestHandler
    extends BaseModifyBeanRequestHandler< AzureDataReplicationRule >
{
    public ModifyAzureDataReplicationRuleRequestHandler()
    {
        super( AzureDataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
                RestDomainType.AZURE_DATA_REPLICATION_RULE );

        registerOptionalBeanProperties(
                DataPersistenceRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES,
                PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES );
    }


    @Override
    protected void modifyBean(
            final CommandExecutionParams params,
            final AzureDataReplicationRule rule,
            final Set< String > modifiedProperties )
    {
        params.getDataPolicyResource().modifyAzureDataReplicationRule(
                rule,
                CollectionFactory.toArray( String.class, modifiedProperties ) ).get( Timeout.LONG );
    }
}
