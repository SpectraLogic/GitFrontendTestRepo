/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;

public interface ImportPublicCloudTargetDirective< T > extends ImportDirective< T >
{
    String CLOUD_BUCKET_NAME = "cloudBucketName";
    
    @Optional
    String getCloudBucketName();
    
    T setCloudBucketName( final String value );
    
    
    String TARGET_ID = "targetId";
    
    UUID getTargetId();
    
    T setTargetId( final UUID value );
    
    
    String PRIORITY = "priority";
    
    @DefaultEnumValue( "NORMAL" )
    BlobStoreTaskPriority getPriority();
    
    T setPriority( final BlobStoreTaskPriority value );
}
