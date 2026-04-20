/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.api;

import java.util.List;

import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.ReadFromObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;

/**
 * A group of {@link JobEntry}s such that no {@link JobEntry} from one group may be mixed with a
 * {@link JobEntry} from another group in the same {@link JobEntry}.  <br><br>
 * 
 * For example, {@link JobEntry}s that require servicing from different {@link Tape}s or {@link Pool}s will
 * always reside in different {@link JobEntryGrouping}s.
 */
public interface JobEntryGrouping extends ReadFromObservable< JobEntryGrouping >
{
    String ENTRIES = "entries";
    
    List<JobEntry> getEntries();
    
    JobEntryGrouping setEntries( final List<JobEntry> entries );
}
