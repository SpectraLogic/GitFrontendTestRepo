/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frmwk;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservableException;

public final class DataPlannerException extends FailureTypeObservableException
{
    public DataPlannerException( final FailureType failureCode, final String message )
    {
        super( failureCode, message );
    }
    
    
    public DataPlannerException( final FailureType failureCode, final String message, final Throwable cause )
    {
        super( failureCode, message, cause );
    }
    
    
    public DataPlannerException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
    
    
    public DataPlannerException( final Throwable cause )
    {
        super( cause );
    }
}
