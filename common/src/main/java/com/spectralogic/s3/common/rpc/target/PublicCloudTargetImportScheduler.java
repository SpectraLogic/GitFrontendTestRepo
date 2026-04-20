/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;

public interface PublicCloudTargetImportScheduler< ID extends ImportPublicCloudTargetDirective< ID > >
{
    void importTarget( final ID directive );
}
