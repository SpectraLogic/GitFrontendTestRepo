/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetDegradedS3DataReplicationRulesRequestHandler
    extends BaseGetBeansRequestHandler< S3DataReplicationRule >
{
    public GetDegradedS3DataReplicationRulesRequestHandler()
    {
        super( S3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.DEGRADED_S3_DATA_REPLICATION_RULE );

        registerOptionalBeanProperties(
                DataPlacement.DATA_POLICY_ID,
                DataPlacement.STATE,
                DataReplicationRule.TARGET_ID,
                DataReplicationRule.TYPE );
    }


    @Override
    protected WhereClause getCustomFilter( 
            final S3DataReplicationRule requestBean, 
            final CommandExecutionParams params )
    {
        return Require.exists( 
                DegradedBlob.class, DegradedBlob.S3_REPLICATION_RULE_ID, Require.nothing() );
    }
}
