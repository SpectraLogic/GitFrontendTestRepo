/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager;

import java.sql.SQLException;

import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.GenericFailure;

public final class DatabaseErrorCodes
{
    public static final String UNIQUE_VIOLATION = "23505";
    public static final String FOREIGN_KEY_VIOLATION = "23503";
    public static final String NOT_NULL_VIOLATION = "23502";


    private DatabaseErrorCodes()
    {
        // singleton
    }
    
    
    public static boolean isExceptionCausedByErrorCode( final Throwable t, final String errorCode )
    {
        if ( null != t.getCause() )
        {
            return isExceptionCausedByErrorCode( t.getCause(), errorCode );
        }
        
        final SQLException sqlException = ExceptionUtil.getExceptionOfType( SQLException.class, t );
        return sqlException != null && sqlException.getSQLState().equals( errorCode );
    }
    
    
    public static void verifyConstraintViolation( final String failureMessage, final Throwable t )
    {
        if ( null != t.getCause() )
        {
            verifyConstraintViolation( failureMessage, t.getCause( ) );
            return;
        }
    
        throwIfUniqueViolation( failureMessage, t );
        throwIfNullViolation( failureMessage, t );
    }
    
    
    private static void throwIfNullViolation( String failureMessage, Throwable t )
    {
        if ( isExceptionCausedByErrorCode( t, NOT_NULL_VIOLATION ) )
        {
            final String prefix1 = "ERROR: ";
            final String prefix2 = "null value ";
            final String suffix = "not-null constraint";
            final String message = t.getMessage( ).substring( t.getMessage( ).indexOf( prefix1 + prefix2 ) )
                    .substring( prefix1.length( ), t.getMessage( ).indexOf( suffix ) + suffix.length( ) );
            
            throw new DaoException( GenericFailure.BAD_REQUEST,
                    "Failed due to not null violation: " + message + "  " + failureMessage );
        }
    }
    
    
    private static void throwIfUniqueViolation( String failureMessage, Throwable t )
    {
        if ( isExceptionCausedByErrorCode( t, UNIQUE_VIOLATION ) )
        {
            final String prefix1 = "Detail: ";
            final String prefix2 = "Key (";
            final String message = t.getMessage( ).substring(
                    t.getMessage().indexOf( prefix1 + prefix2 ) ).substring( prefix1.length() );
    
            throw new DaoException(
                    GenericFailure.CONFLICT,
                    "Failed due to unique constraint violation: " + message + "  " + failureMessage );
        }
    }
}
