/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.FailureService;

public interface TapePartitionFailureService extends FailureService< TapePartitionFailure >
{
    void create( 
            final UUID partitionId,
            final TapePartitionFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void create(
            final UUID partitionId,
            final TapePartitionFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void deleteAll( final UUID partitionId, final TapePartitionFailureType type );
    
    
    void deleteAll( final UUID partitionId );
    
    
    ActiveFailures startActiveFailures( final UUID partitionId, final TapePartitionFailureType type );
}
