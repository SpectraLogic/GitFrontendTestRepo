/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetAzureDataReplicationRulesRequestHandler
    extends BaseGetBeansRequestHandler< AzureDataReplicationRule >
{
    public GetAzureDataReplicationRulesRequestHandler()
    {
        super( AzureDataReplicationRule.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
                RestDomainType.AZURE_DATA_REPLICATION_RULE );
    
        registerOptionalBeanProperties(
                DataPlacement.DATA_POLICY_ID,
                DataPlacement.STATE,
                DataReplicationRule.TARGET_ID,
                DataReplicationRule.TYPE,
                DataReplicationRule.REPLICATE_DELETES );
    }
}
