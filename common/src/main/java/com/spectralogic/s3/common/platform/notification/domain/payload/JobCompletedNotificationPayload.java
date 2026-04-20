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

public interface JobCompletedNotificationPayload extends JobCreatedNotificationPayload
{
    String CANCEL_OCCURRED = "cancelOccurred";
    
    boolean isCancelOccurred();
    
    void setCancelOccurred( final boolean value );
    
    
    String OBJECTS_NOT_PERSISTED = "objectsNotPersisted";

    @CustomMarshaledName( 
            collectionValue = "ObjectsNotPersisted",
            value = "Object",
            collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
    BlobApiBean [] getObjectsNotPersisted();
    
    void setObjectsNotPersisted( final BlobApiBean [] value );
}
