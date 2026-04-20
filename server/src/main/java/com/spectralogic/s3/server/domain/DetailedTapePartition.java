/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.marshal.CustomMarshaledName;

public interface DetailedTapePartition extends TapePartition
{
    String TAPE_TYPES = "tapeTypes";
    
    TapeType [] getTapeTypes();
    
    void setTapeTypes( final TapeType [] value );
    
    
    String DRIVE_TYPES = "driveTypes";
    
    TapeDriveType [] getDriveTypes();
    
    void setDriveTypes( final TapeDriveType [] value );


    String TOTAL_STORAGE_CAPACITY = "totalStorageCapacity";

    long getTotalStorageCapacity();

    void setTotalStorageCapacity( final long value );


    String USED_STORAGE_CAPACITY = "usedStorageCapacity";

    long getUsedStorageCapacity();

    void setUsedStorageCapacity( final long value );


    String AVAILABLE_STORAGE_CAPACITY = "availableStorageCapacity";

    long getAvailableStorageCapacity();

    void setAvailableStorageCapacity( final long value );


    String TAPE_COUNT = "tapeCount";

    int getTapeCount();

    void setTapeCount( final int value );


    String TAPE_TYPE_SUMMARIES = "tapeTypeSummaries";

    @CustomMarshaledName(
            value = "TapeTypeSummary",
            collectionValue = "TapeTypeSummaries",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    TapeTypeSummaryApiBean [] getTapeTypeSummaries();

    void setTapeTypeSummaries( final TapeTypeSummaryApiBean [] value );


    String TAPE_STATE_SUMMARIES = "tapeStateSummaries";

    @CustomMarshaledName(
            value = "TapeStateSummary",
            collectionValue = "TapeStateSummaries",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    TapeStateSummaryApiBean[] getTapeStateSummaries();

    void setTapeStateSummaries(final TapeStateSummaryApiBean[] value );

}