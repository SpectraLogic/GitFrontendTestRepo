/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface S3ObjectToDeleteApiBean extends SimpleBeanSafeToProxy
{
    String KEY = "key";
    
    String getKey();
    
    void setKey( final String value );
    
    
    String VERSION_ID = "versionId";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    UUID getVersionId();
    
    void setVersionId( final UUID value );
}
