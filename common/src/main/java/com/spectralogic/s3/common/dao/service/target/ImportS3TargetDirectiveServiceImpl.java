/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import com.spectralogic.s3.common.dao.domain.target.ImportS3TargetDirective;

final class ImportS3TargetDirectiveServiceImpl
    extends BaseImportTargetDirectiveService< ImportS3TargetDirective >
    implements ImportS3TargetDirectiveService
{
    ImportS3TargetDirectiveServiceImpl()
    {
        super( ImportS3TargetDirective.class );
    }
}