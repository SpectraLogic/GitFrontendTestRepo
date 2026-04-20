/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import java.util.List;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.dataplanner.frontend.api.JobEntryGrouping;

public interface WriteDataOrderingStrategy
{
    /**
     * @return {@link JobEntryGrouping}s to create {@link JobEntry}s for
     */
    List< JobEntryGrouping > order( final Set< S3Object > objectsToWrite, final Set< Blob > blobsToWrite );
}
