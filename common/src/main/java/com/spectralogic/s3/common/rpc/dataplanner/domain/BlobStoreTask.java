/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface BlobStoreTask
{
    String ID = "id";
    
    long getId();
    
    
    String PRIORITY = "priority";
    
    BlobStoreTaskPriority getPriority();
    
    BlobStoreTask setPriority( final BlobStoreTaskPriority value );
    
    
    String STATE = "state";
    
    BlobStoreTaskState getState();
    
    
    String DRIVE_ID = "driveId";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getDriveId();
    
    
    String TAPE_ID = "tapeId";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getTapeId();
    
    
    String POOL_ID = "poolId";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getPoolId();
    
    
    String TARGET_TYPE = "targetType";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    String getTargetType();
    
    
    String TARGET_ID = "targetId";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getTargetId();
    
    
    String NAME = "name";
    
    String getName();
    
    
    String DESCRIPTION = "description";
    
    String getDescription();
    
    
    String DATE_SCHEDULED = "dateScheduled";
    
    Date getDateScheduled();
    
    
    String DATE_STARTED = "dateStarted";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Date getDateStarted();


    String JOB_IDS = "jobIds";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    @CustomMarshaledName(
            value = "JobIds",
            collectionValue = "JobId",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    default UUID[] getJobIds() { return null; }
    
    
    String DURATION_SCHEDULED = "durationScheduled";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Duration getDurationScheduled();
    
    
    String DURATION_IN_PROGRESS = "durationInProgress";

    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Duration getDurationInProgress();


}
