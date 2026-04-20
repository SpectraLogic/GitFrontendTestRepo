/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;


public final class DaoException extends FailureTypeObservableException
{
    public DaoException( final FailureType failureCode, final String message )
    {
        super( failureCode, message );
    }
    
    
    public DaoException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
    
    
    public DaoException( final Throwable cause )
    {
        super( cause );
    }
    
    
    public DaoException( final FailureType failureCode, final String message, final Throwable cause )
    {
        super( failureCode, message, cause );
    }
}
