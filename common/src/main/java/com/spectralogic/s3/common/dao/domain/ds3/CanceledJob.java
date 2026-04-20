/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( { @Index( CanceledJob.DATE_CANCELED ), @Index( CanceledJob.CREATED_AT ), @Index( CanceledJob.NAME ) } )
public interface CanceledJob extends DatabasePersistable, JobObservable< CanceledJob >
{
    @CascadeDelete
    @References( User.class )
    UUID getUserId();
    
    
    String DATE_CANCELED = "dateCanceled";
    
    @SortBy( direction = Direction.DESCENDING, value = 0 )
    @DefaultToCurrentDate
    Date getDateCanceled();
    
    CanceledJob setDateCanceled( final Date value );
    
    
    String CANCELED_DUE_TO_TIMEOUT = "canceledDueToTimeout";
    
    @DefaultBooleanValue( false )
    boolean isCanceledDueToTimeout();
    
    CanceledJob setCanceledDueToTimeout( final boolean value );
}
