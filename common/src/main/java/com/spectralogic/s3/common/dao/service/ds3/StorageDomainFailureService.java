/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.FailureService;

public interface StorageDomainFailureService extends FailureService< StorageDomainFailure >
{
    void create( 
            final UUID storageDomainId,
            final StorageDomainFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void create(
            final UUID storageDomainId,
            final StorageDomainFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void deleteAll( final UUID storageDomainId, final StorageDomainFailureType type );
    
    
    void deleteAll( final UUID storageDomainId );
    
    
    ActiveFailures startActiveFailures( final UUID storageDomainId, final StorageDomainFailureType type );
}
