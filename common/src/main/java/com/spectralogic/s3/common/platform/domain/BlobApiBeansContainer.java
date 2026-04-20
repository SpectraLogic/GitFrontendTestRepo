/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledName;

public interface BlobApiBeansContainer extends SimpleBeanSafeToProxy
{
    String OBJECTS = "objects";

    @CustomMarshaledName( "object" )
    BlobApiBean [] getObjects();
    
    void setObjects( final BlobApiBean [] value );
}
