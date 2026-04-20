/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.lang;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservableException;

public final class SimulatorException extends FailureTypeObservableException
{
    public SimulatorException( final FailureType failureCode, final String message )
    {
        super( failureCode, message );
    }
    
    
    public SimulatorException( final FailureType failureCode, final String message, final Throwable cause )
    {
        super( failureCode, message, cause );
    }
    
    
    public SimulatorException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
    
    
    public SimulatorException( final Throwable cause )
    {
        super( cause );
    }
}
