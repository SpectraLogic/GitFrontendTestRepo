/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;

public final class GetDataPersistenceRulesRequestHandler
    extends BaseGetBeansRequestHandler< DataPersistenceRule >
{
    public GetDataPersistenceRulesRequestHandler()
    {
        super( DataPersistenceRule.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.DATA_PERSISTENCE_RULE );
        
        registerOptionalBeanProperties(
                DataPlacement.DATA_POLICY_ID,
                DataPlacement.STATE,
                DataPersistenceRule.STORAGE_DOMAIN_ID,
                DataPersistenceRule.TYPE,
                DataPersistenceRule.ISOLATION_LEVEL );
    }
}
