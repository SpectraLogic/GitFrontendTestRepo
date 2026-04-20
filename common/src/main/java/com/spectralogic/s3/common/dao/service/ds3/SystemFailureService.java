/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.FailureService;

public interface SystemFailureService extends FailureService< SystemFailure >
{
    void create( 
            final SystemFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void create(
            final SystemFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void deleteAll( final SystemFailureType type );
    
    
    ActiveFailures startActiveFailures( final SystemFailureType type );
}
