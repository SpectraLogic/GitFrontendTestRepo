/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.db.lang.References;

public interface UserIdObservable< T >
{
    String USER_ID = "userId";
    
    @References( User.class )
    UUID getUserId();
    
    T setUserId( final UUID value );
}
