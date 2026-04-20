/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.S3TargetReadPreference;
import com.spectralogic.util.db.service.BaseService;

final class S3TargetReadPreferenceServiceImpl
    extends BaseService< S3TargetReadPreference >
    implements S3TargetReadPreferenceService
{
    S3TargetReadPreferenceServiceImpl()
    {
        super( S3TargetReadPreference.class );
    }
}
