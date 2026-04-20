/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import com.spectralogic.util.db.domain.Mutex;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.CannotBeUsedOnTransactions;

@CannotBeUsedOnTransactions
public interface MutexService extends BeansRetriever< Mutex >
{
    void run( final String lockName, final Runnable r );
}
