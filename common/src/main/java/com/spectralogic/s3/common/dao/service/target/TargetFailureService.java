/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.shared.FailureService;
import com.spectralogic.util.db.lang.DatabasePersistable;

public interface TargetFailureService
    < T extends Failure< T, TargetFailureType > & DatabasePersistable & TargetFailure< T > >
    extends FailureService< T >
{
    void create( 
            final UUID targetId,
            final TargetFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void create(
            final UUID targetId,
            final TargetFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void deleteAll( final UUID targetId, final TargetFailureType type );
    
    
    void deleteAll( final UUID targetId );
}
