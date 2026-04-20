/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDs3DataReplicationRulesRequestHandler
    extends BaseGetBeansRequestHandler< Ds3DataReplicationRule >
{
    public GetDs3DataReplicationRulesRequestHandler()
    {
        super( Ds3DataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.DS3_DATA_REPLICATION_RULE );

        registerOptionalBeanProperties(
                DataPlacement.DATA_POLICY_ID,
                DataPlacement.STATE,
                DataReplicationRule.TARGET_ID,
                DataReplicationRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES );
    }
}
