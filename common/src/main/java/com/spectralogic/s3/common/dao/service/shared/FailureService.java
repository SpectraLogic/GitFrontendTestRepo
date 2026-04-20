/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.shared;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface FailureService< T extends DatabasePersistable & Failure< T, ? > >
    extends BeansRetriever< T >, BeanDeleter
{
    Set< String > getFailureDescribingProps();
    
    
    void create( final T failure, final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void deleteAll();
    

    void deleteOldFailures();
    void deleteOldFailures( final long failureAgeToBeOldInMillis );
    int DEFAULT_FAILURE_AGE_TO_BE_OLD_IN_SECS = 30 * 24 * 60 * 60;
}
