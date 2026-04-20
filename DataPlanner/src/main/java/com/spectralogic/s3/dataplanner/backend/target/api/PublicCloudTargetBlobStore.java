/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.api;

import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.rpc.target.PublicCloudTargetImportScheduler;

public interface PublicCloudTargetBlobStore< ID extends ImportPublicCloudTargetDirective< ID > > 
    extends TargetBlobStore, PublicCloudTargetImportScheduler< ID >
{
    // empty
}
