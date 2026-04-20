/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.api;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

public final class PoolLockingException extends FailureTypeObservableException
{
    public PoolLockingException( final String message, final RuntimeException ex )
    {
        super( GenericFailure.CANNOT_LOCK_AT_THIS_TIME, message, ex );
    }
}
