/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetDegradedAzureDataReplicationRulesRequestHandler
    extends BaseGetBeansRequestHandler< AzureDataReplicationRule >
{
    public GetDegradedAzureDataReplicationRulesRequestHandler()
    {
        super( AzureDataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.DEGRADED_AZURE_DATA_REPLICATION_RULE );

        registerOptionalBeanProperties(
                DataPlacement.DATA_POLICY_ID,
                DataPlacement.STATE,
                DataReplicationRule.TARGET_ID,
                DataReplicationRule.TYPE );
    }


    @Override
    protected WhereClause getCustomFilter( 
            final AzureDataReplicationRule requestBean, 
            final CommandExecutionParams params )
    {
        return Require.exists( 
                DegradedBlob.class, DegradedBlob.AZURE_REPLICATION_RULE_ID, Require.nothing() );
    }
}
