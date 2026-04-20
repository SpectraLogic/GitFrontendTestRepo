/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;

public final class ForceFlagRequiredException extends FailureTypeObservableException
{
    public ForceFlagRequiredException( final String cause )
    {
        super( GenericFailure.FORCE_FLAG_REQUIRED,
               "The force flag must be used since " + cause + "." );
    }
    
    
    public ForceFlagRequiredException( final Throwable cause )
    {
        super( GenericFailure.FORCE_FLAG_REQUIRED, 
               "The force flag must be used since " + cause.getMessage() + ".", cause );
    }
}
