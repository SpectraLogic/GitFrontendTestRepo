/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;

public interface BlobTarget< T > extends BlobObservable< T >
{
    String TARGET_ID = "targetId";
    
    UUID getTargetId();
    
    T setTargetId( final UUID value );
}
