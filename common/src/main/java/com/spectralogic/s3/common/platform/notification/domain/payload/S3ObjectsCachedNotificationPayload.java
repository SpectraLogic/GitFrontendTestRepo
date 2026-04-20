/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.domain.payload;

import com.spectralogic.s3.common.platform.domain.BlobApiBean;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;

public interface S3ObjectsCachedNotificationPayload extends JobCreatedNotificationPayload
{
    String OBJECTS = "objects";
    
    @CustomMarshaledName( 
            collectionValue = "Objects",
            value = "Object",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    BlobApiBean [] getObjects();
    
    void setObjects( final BlobApiBean [] value );
}
