/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;

public interface ReplicationTargetImportHandler extends ImportHandler< TargetFailureType >
{
    /**
     * Finalizes an import, taking the final steps to take ownership of the bucket imported.
     */
    BlobStoreTaskState finalizeImport();
}
