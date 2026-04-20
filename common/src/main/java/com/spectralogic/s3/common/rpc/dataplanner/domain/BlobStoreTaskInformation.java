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
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BlobStoreTaskInformation extends BlobStoreTask, SimpleBeanSafeToProxy
{
    BlobStoreTaskInformation setId( final long value );
    
    BlobStoreTaskInformation setPriority( final BlobStoreTaskPriority value );
    
    BlobStoreTaskInformation setState( final BlobStoreTaskState value );
    
    BlobStoreTaskInformation setDriveId( final UUID value );
    
    BlobStoreTaskInformation setTapeId( final UUID value );
    
    BlobStoreTaskInformation setPoolId( final UUID value );
    
    BlobStoreTaskInformation setTargetType( final String value );
    
    BlobStoreTaskInformation setTargetId( final UUID value );
    
    BlobStoreTaskInformation setName( final String value );
    
    BlobStoreTaskInformation setDescription( final String value );
    
    BlobStoreTaskInformation setDateScheduled( final Date value );
    
    BlobStoreTaskInformation setDateStarted( final Date value );

    BlobStoreTaskInformation setJobIds( final UUID[] jobIds);
}
