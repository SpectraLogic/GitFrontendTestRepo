/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.util.List;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;

public interface ChunkReadingTask
{
    List<JobEntry> getEntries();
}
