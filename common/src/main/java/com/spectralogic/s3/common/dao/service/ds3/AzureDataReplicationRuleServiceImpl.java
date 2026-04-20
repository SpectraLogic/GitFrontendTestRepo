/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.util.db.service.BaseService;

final class AzureDataReplicationRuleServiceImpl
    extends BaseService< AzureDataReplicationRule > implements AzureDataReplicationRuleService
{
    AzureDataReplicationRuleServiceImpl()
    {
        super( AzureDataReplicationRule.class );
    }
}
