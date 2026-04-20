/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.util.db.service.BaseService;

final class S3DataReplicationRuleServiceImpl
    extends BaseService< S3DataReplicationRule > implements S3DataReplicationRuleService
{
    S3DataReplicationRuleServiceImpl()
    {
        super( S3DataReplicationRule.class );
    }
}
