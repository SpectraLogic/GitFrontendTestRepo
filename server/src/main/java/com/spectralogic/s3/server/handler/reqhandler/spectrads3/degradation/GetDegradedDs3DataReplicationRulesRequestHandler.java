/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetDegradedDs3DataReplicationRulesRequestHandler
    extends BaseGetBeansRequestHandler< Ds3DataReplicationRule >
{
    public GetDegradedDs3DataReplicationRulesRequestHandler()
    {
        super( Ds3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.DEGRADED_DS3_DATA_REPLICATION_RULE );
         
         registerOptionalBeanProperties(
                 DataPlacement.DATA_POLICY_ID,
                 DataPlacement.STATE,
                 DataReplicationRule.TARGET_ID,
                 DataReplicationRule.TYPE );
     }
    
    
    @Override
    protected WhereClause getCustomFilter( 
            final Ds3DataReplicationRule requestBean, 
            final CommandExecutionParams params )
    {
        return Require.exists( DegradedBlob.class, DegradedBlob.DS3_REPLICATION_RULE_ID, Require.nothing() );
    }
}
