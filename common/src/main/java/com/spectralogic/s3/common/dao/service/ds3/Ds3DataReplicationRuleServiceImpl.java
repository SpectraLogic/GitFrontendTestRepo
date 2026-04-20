/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.util.db.service.BaseService;

final class Ds3DataReplicationRuleServiceImpl
    extends BaseService< Ds3DataReplicationRule > implements Ds3DataReplicationRuleService
{
    Ds3DataReplicationRuleServiceImpl()
    {
        super( Ds3DataReplicationRule.class );
    }
}
