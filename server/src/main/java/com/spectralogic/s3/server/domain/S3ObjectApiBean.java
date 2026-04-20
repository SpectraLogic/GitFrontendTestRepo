/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface S3ObjectApiBean extends SimpleBeanSafeToProxy
{
    String KEY = "key";
    
    String getKey();
    
    void setKey( final String key );
    
    
    String LAST_MODIFIED = "lastModified";
    
    @Optional
    Date getLastModified();
    
    void setLastModified( final Date value );
    
    
    String E_TAG = "eTag";
    
    @Optional
    String getETag();
    
    void setETag( final String value );
    
    
    String SIZE = "size";
    
    long getSize();
    
    void setSize( final long value );
    
    
    String STORAGE_CLASS = "storageClass";
    
    @Optional
    Object getStorageClass();
    
    
    String OWNER = "owner";
    
    UserApiBean getOwner();
    
    void setOwner( final UserApiBean value );
    
    
    String VERSION_ID = "versionId";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getVersionId();
    
    void setVersionId( final UUID value );
    
    
    String IS_LATEST = "isLatest";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    Boolean getIsLatest();
    
    void setIsLatest( final Boolean value );
}
