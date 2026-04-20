/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

/**
 * A successfully completed job.  A job that was cancelled is not said to be successfully completed.
 */
@Indexes( { @Index( CompletedJob.DATE_COMPLETED ), @Index( CompletedJob.CREATED_AT ), @Index( CompletedJob.NAME ) } )
public interface CompletedJob extends DatabasePersistable, JobObservable< CompletedJob >
{
    @CascadeDelete
    @References( User.class )
    UUID getUserId();
    
    
    String DATE_COMPLETED = "dateCompleted";
    
    @SortBy( direction = Direction.DESCENDING, value = 0 ) 
    @DefaultToCurrentDate
    Date getDateCompleted();
    
    CompletedJob setDateCompleted( final Date value );
}
