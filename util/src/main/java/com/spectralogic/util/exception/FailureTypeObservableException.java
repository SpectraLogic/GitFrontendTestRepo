/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;

public class FailureTypeObservableException extends RuntimeException implements FailureTypeObservable
{
    public FailureTypeObservableException( final FailureType failureCode, final String message )
    {
        super( LogUtil.getShortVersion( message ) );
        m_failureType = failureCode;
        Validations.verifyNotNull( "Failure type", m_failureType );
    }
    
    
    public FailureTypeObservableException( final String message, final Throwable cause )
    {
        super( LogUtil.getShortVersion( message ), cause );
        m_failureType = ( FailureTypeObservable.class.isAssignableFrom( cause.getClass() ) ) ?
                ( (FailureTypeObservable)cause ).getFailureType()
                : GenericFailure.INTERNAL_ERROR;
        Validations.verifyNotNull( "Failure type", m_failureType );
    }
    
    
    public FailureTypeObservableException( final Throwable cause )
    {
        super( cause );
        m_failureType = ( FailureTypeObservable.class.isAssignableFrom( cause.getClass() ) ) ?
                ( (FailureTypeObservable)cause ).getFailureType()
                : GenericFailure.INTERNAL_ERROR;
                Validations.verifyNotNull( "Failure type", m_failureType );
    }
    
    
    /**
     * <font color = red><b>
     * Avoid using this constructor.  If there is a cause, the failure type is usually an internal error.
     * </b></font>
     */
    public FailureTypeObservableException(
            final FailureType failureType, 
            final String message,
            final Throwable cause )
    {
        super( message, cause );
        m_failureType = failureType;
        Validations.verifyNotNull( "Failure type", m_failureType );
    }
    
    
    final public FailureType getFailureType()
    {
        return m_failureType;
    }
    
    
    private final FailureType m_failureType;
}
