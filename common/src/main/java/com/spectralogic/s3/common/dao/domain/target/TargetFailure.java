/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.DatabasePersistable;

public interface TargetFailure< T extends DatabasePersistable > extends Failure< T, TargetFailureType >
{
    String TARGET_ID = "targetId";
    
    UUID getTargetId();
    
    T setTargetId( final UUID value );
    
    
    TargetFailureType getType();
    
    T setType( final TargetFailureType value );
}
