/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.dataplanner.frontend.api.JobEntryGrouping;

public interface ReadDataOrderingStrategy
{
    /**
     * @return {@link JobEntryGrouping}s to create {@link JobEntry}s for
     */
    List< JobEntryGrouping > order();
    
    
    /**
     * @return {@link UUID}s of any blobs which are not available to be read from anywhere
     */
    Set< UUID > getUnavailableBlobs();
}
