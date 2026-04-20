/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.Ds3TargetReadPreference;
import com.spectralogic.util.db.service.BaseService;

final class Ds3TargetReadPreferenceServiceImpl
    extends BaseService< Ds3TargetReadPreference >
    implements Ds3TargetReadPreferenceService
{
    Ds3TargetReadPreferenceServiceImpl()
    {
        super( Ds3TargetReadPreference.class );
    }
}
