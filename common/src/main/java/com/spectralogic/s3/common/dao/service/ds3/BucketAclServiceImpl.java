/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.util.db.service.BaseService;

final class BucketAclServiceImpl extends BaseService< BucketAcl > implements BucketAclService
{
    BucketAclServiceImpl()
    {
        super( BucketAcl.class );
    }
}
