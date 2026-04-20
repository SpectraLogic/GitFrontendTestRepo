/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetDegradedDataPersistenceRulesRequestHandler 
    extends BaseGetBeansRequestHandler< DataPersistenceRule >
{
    public GetDegradedDataPersistenceRulesRequestHandler()
    {
        super( DataPersistenceRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.DEGRADED_DATA_PERSISTENCE_RULE );
         
         registerOptionalBeanProperties(
                 DataPlacement.DATA_POLICY_ID,
                 DataPlacement.STATE,
                 DataPersistenceRule.STORAGE_DOMAIN_ID,
                 DataPersistenceRule.TYPE,
                 DataPersistenceRule.ISOLATION_LEVEL );
     }
    
    
    @Override
    protected WhereClause getCustomFilter( 
            final DataPersistenceRule requestBean, 
            final CommandExecutionParams params )
    {
        return Require.exists( DegradedBlob.class, DegradedBlob.PERSISTENCE_RULE_ID, Require.nothing() );
    }
}
